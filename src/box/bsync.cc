/*
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#define MH_SOURCE 1
#include "box/bsync.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include "cfg.h"
#include "fio.h"
#include "coio.h"
#include "coio_buf.h"
#include "scoped_guard.h"
#include "box/box.h"
#include "box/port.h"
#include "box/log_io.h"
#include "box/request.h"
#include "msgpuck/msgpuck.h"

#include "box/bsync_hash.h"

#define BSYNC_SYSBUFFER_SIZE 64
#define BSYNC_MAX_HOSTS 14
#define BSYNC_TRACE \
	say_debug("%s:%d current state: nconnected=%d, state=%d, naccepted=%d", \
	__PRETTY_FUNCTION__, __LINE__, bsync_state.num_connected, \
	bsync_state.state, bsync_state.num_accepted);

static void* bsync_thread(void*);
static void bsync_process_fiber(va_list ap);
static void bsync_connected(uint8_t host_id);

static struct coio_service bsync_coio;
static struct wal_writer* wal_local_writer = NULL;
static struct wal_writer proxy_wal_writer;
static struct recovery_state *recovery_state;
static pthread_once_t bsync_writer_once = PTHREAD_ONCE_INIT;

static struct ev_loop* txn_loop;
static struct ev_loop* bsync_loop;
static struct ev_async txn_process_event;
static struct ev_async bsync_process_event;

struct bsync_op_status {
	struct bsync_operation* op;

	struct rlist list;
};

enum bsync_operation_status {
	bsync_op_status_init = 0,
	bsync_op_status_accept = 1,
	bsync_op_status_wal = 2,
	bsync_op_status_submit = 3,
	bsync_op_status_yield = 4
};

struct bsync_operation {
	uint64_t gsn;
	uint64_t lsn;
	uint8_t server_id;
	uint8_t status;
	uint8_t accepted;
	uint8_t rejected;
	int result;
	struct fiber* bsync_owner;
	struct fiber* txn_owner;
	struct xrow_header* row;

	struct rlist list;
	STAILQ_ENTRY(bsync_operation) fifo;
};
STAILQ_HEAD(bsync_fifo, bsync_operation);

struct bsync_send_elem {
	uint8_t code;
	struct bsync_operation* oper;

	struct rlist state;
};

enum bsync_host_flags {
	bsync_host_active_read = 0x01,
	bsync_host_active_write = 0x02
};

struct bsync_host_data {
	uint8_t connected;
	uint8_t flags;
	uint64_t gsn;
	char source[REMOTE_SOURCE_MAXLEN];
	struct uri uri;
	struct fiber* fiber_out;
	struct fiber* fiber_in;
	const void* buffer_out;
	ssize_t buffer_out_size;

	struct mh_strptr_t *active_ops;

	struct rlist send_queue;
	struct rlist op_queue;
};
static struct bsync_host_data bsync_index[BSYNC_MAX_HOSTS];

static struct bsync_state_ {
	uint8_t local_id;
	uint8_t leader_id;
	uint64_t lsn;
	uint8_t num_hosts;
	uint8_t num_connected;
	uint8_t state;
	uint8_t num_accepted;

	uint64_t recovery_hosts;
	ev_tstamp connect_timeout;
	ev_tstamp read_timeout;
	ev_tstamp write_timeout;
	ev_tstamp reconnect_timeout;
	ev_tstamp operation_timeout;
	ev_tstamp slow_host_timeout;
	ev_tstamp ping_timeout;
	ev_tstamp election_timeout;

	const char** iproxy_pos;
	const char* iproxy_end;

	struct rlist accept_queue;
	struct rlist submit_queue;
	struct rlist txn_queue;
	struct rlist execute_queue;

	struct rlist txn_fiber_cache;
	struct rlist bsync_fiber_cache;

	struct bsync_fifo txn_proxy_queue;
	struct bsync_fifo txn_proxy_input;

	struct bsync_fifo bsync_proxy_queue;
	struct bsync_fifo bsync_proxy_input;
} bsync_state;

static struct fiber*
bsync_fiber(struct rlist *lst, void (*f) (va_list))
{
	if (! rlist_empty(lst))
		return rlist_shift_entry(lst, struct fiber, state);
	else
		return fiber_new("bsync_proc", f);
}

static uint8_t
bsync_max_host()
{BSYNC_TRACE
	uint8_t max_host_id = 0;
	for (uint8_t i = 1; i < bsync_state.num_hosts; ++i) {
		if (bsync_index[i].gsn >= bsync_index[max_host_id].gsn &&
			bsync_index[i].connected == 2)
		{
			max_host_id = i;
		}
	}
	return max_host_id;
}

#define BSYNC_LOCAL bsync_index[bsync_state.local_id]
#define BSYNC_LEADER bsync_index[bsync_state.leader_id]
#define BSYNC_REMOTE bsync_index[host_id]

static char bsync_system_out[BSYNC_SYSBUFFER_SIZE];
static char bsync_system_in[BSYNC_SYSBUFFER_SIZE];

enum bsync_message_type {
	bsync_mtype_hello = 0,
	bsync_mtype_leader_promise = 1,
	bsync_mtype_leader_accept = 2,
	bsync_mtype_leader_submit = 3,
	bsync_mtype_leader_reject = 4,
	bsync_mtype_body = 5,
	bsync_mtype_submit = 6,
	bsync_mtype_reject = 7,
	bsync_mtype_proxy_request = 8,
	bsync_mtype_proxy_accept = 9,
	bsync_mtype_proxy_submit = 10,
	bsync_mtype_proxy_reject = 11,
	bsync_mtype_ping = 12,
	bsync_mtype_count = 13
};

enum bsync_machine_state {
	bsync_state_started = 0,
	bsync_state_initial = 1,
	bsync_state_leader_accept = 2,
	bsync_state_recovery = 3,
	bsync_state_wait_recovery = 4,
	bsync_state_ready = 5
};

static const char* bsync_mtype_name[] = {
	"bsync_hello",
	"bsync_leader_promise",
	"bsync_leader_accept",
	"bsync_leader_submit",
	"bsync_leader_reject",
	"bsync_body",
	"bsync_submit",
	"bsync_reject",
	"bsync_proxy_request",
	"bsync_proxy_accept",
	"bsync_proxy_submit",
	"bsync_proxy_reject",
	"bsync_ping"
};

#define SWITCH_TO_BSYNC {\
	tt_pthread_mutex_lock(&proxy_wal_writer.mutex); \
	bool was_empty = STAILQ_EMPTY(&bsync_state.bsync_proxy_queue); \
	STAILQ_INSERT_TAIL(&bsync_state.bsync_proxy_queue, oper, fifo); \
	if (was_empty) ev_async_send(bsync_loop, &bsync_process_event); \
	tt_pthread_mutex_unlock(&proxy_wal_writer.mutex); }

#define SWITCH_TO_TXN {\
	tt_pthread_mutex_lock(&proxy_wal_writer.mutex); \
	bool was_empty = STAILQ_EMPTY(&bsync_state.txn_proxy_queue); \
	STAILQ_INSERT_TAIL(&bsync_state.txn_proxy_queue, oper, fifo); \
	if (was_empty) ev_async_send(txn_loop, &txn_process_event); \
	tt_pthread_mutex_unlock(&proxy_wal_writer.mutex); }

int
bsync_write(struct recovery_state *r, struct xrow_header *row) try
{BSYNC_TRACE
	if (wal_local_writer == NULL) {
		return wal_write_lsn(r, row);
	}
	row->tm = ev_now(loop());
	row->sync = 0;
	struct bsync_operation *oper = NULL;
	struct bsync_op_status *status = NULL;
	bool local_request = (row->server_id == 0);
	if (local_request) { /* local request */
		oper = (struct bsync_operation *)
			region_alloc(&fiber()->gc, sizeof(struct bsync_operation));
		status = (struct bsync_op_status *)
			region_alloc(&fiber()->gc, sizeof(struct bsync_op_status));
		status->op = oper;
		oper->row = row;
		oper->result = -1;
		oper->status = bsync_op_status_init;
		oper->server_id = 0;
		oper->bsync_owner = NULL;
		rlist_add_tail_entry(&bsync_state.execute_queue, status, list);
	} else { /* proxy request */
		oper = rlist_shift_entry(&bsync_state.txn_queue,
					 struct bsync_operation, list);
	}
	oper->txn_owner = fiber();
	SWITCH_TO_BSYNC
	fiber_yield();BSYNC_TRACE
	if (status) {
		struct bsync_op_status *s =
			rlist_first_entry(&bsync_state.execute_queue,
					  struct bsync_op_status, list);
		assert(s == status);
		if (s->op->result >= 0) {
			rlist_shift_entry(&bsync_state.execute_queue,
					  struct bsync_op_status, list);
			return oper->result;
		}
		/* rollback in reverse order local operations */
		rlist_foreach_entry_reverse(s, &bsync_state.execute_queue, list) {
			s->op->result = -1;
			fiber_call(s->op->txn_owner);
		}
		rlist_create(&bsync_state.execute_queue);
	}
	return oper->result;
} catch (...) {
	say_crit("bsync_write found unhandled exception");
	throw;
}

static void
bsync_send_data(struct bsync_host_data *host, struct bsync_send_elem *elem) {
	rlist_add_tail_entry(&host->send_queue, elem, state);
	if ((host->flags & bsync_host_active_write) == 0) {
		fiber_call(host->fiber_out);
	}
}

static void
bsync_queue_slave(struct bsync_operation *oper)
{BSYNC_TRACE
	struct bsync_send_elem *elem = (struct bsync_send_elem *)
		region_alloc(&fiber()->gc, sizeof(bsync_send_elem));
	oper->bsync_owner = fiber();
	oper->gsn = 0;
	oper->row->server_id = bsync_state.local_id + 1;
	elem->code = bsync_mtype_proxy_request;
	elem->oper = oper;
	oper->result = 0;
	rlist_add_tail_entry(&bsync_state.accept_queue, oper, list);
	bsync_send_data(&BSYNC_LEADER, elem);
	/* wait accept or reject */
	fiber_yield();BSYNC_TRACE
	if (oper->result < 0) {
		SWITCH_TO_TXN
		return;
	}
	oper->status = bsync_op_status_accept;
	oper->row->server_id = BSYNC_SERVER_ID;
	oper->row->lsn = oper->gsn;
	rlist_add_tail_entry(&bsync_state.submit_queue, oper, list);
	int wal_result = wal_write(wal_local_writer, oper->row);
	elem->code = wal_result < 0 ? bsync_mtype_reject
				    : bsync_mtype_submit;
	bsync_send_data(&BSYNC_LEADER, elem);
	if (oper->status == bsync_op_status_accept) {
		oper->status = bsync_op_status_wal;
		oper->result = wal_result;
		fiber_yield();BSYNC_TRACE
	}
	SWITCH_TO_TXN
}

static void
bsync_wait_slow(struct bsync_operation *oper)
{BSYNC_TRACE
	ev_tstamp start = ev_now(loop());
	while ((oper->accepted + oper->rejected) < bsync_state.num_hosts) {
		oper->status = bsync_op_status_yield;
		fiber_yield_timeout(bsync_state.slow_host_timeout);BSYNC_TRACE
		if (ev_now(loop()) - start <= bsync_state.slow_host_timeout)
			continue;
		/* disconnect slow nodes */
		break;
	}
	BSYNC_TRACE
}

static void
bsync_queue_leader(struct bsync_operation *oper, bool proxy)
{BSYNC_TRACE
	oper->status = bsync_op_status_init;
	oper->gsn = ++BSYNC_LOCAL.gsn;
	oper->result = wal_write(wal_local_writer, oper->row);
	if (oper->result < 0) {
		oper->rejected = 1;
		oper->accepted = 0;
	} else {
		oper->rejected = 0;
		oper->accepted = 1;
	}
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		if (BSYNC_REMOTE.connected < 2 ||
			host_id == bsync_state.local_id)
			continue;
		struct bsync_send_elem *elem = (struct bsync_send_elem *)
			region_alloc(&fiber()->gc, sizeof(bsync_send_elem));
		elem->oper = oper;
		struct bsync_op_status *op_status = (struct bsync_op_status *)
			region_alloc(&fiber()->gc, sizeof(bsync_op_status));
		op_status->op = oper;
		if (oper->server_id == (host_id + 1)) {
			elem->code = bsync_mtype_proxy_accept;
			oper->row->lsn = oper->lsn;
			oper->row->server_id = oper->server_id;
		} else {
			elem->code = bsync_mtype_body;
			oper->row->lsn = oper->gsn;
			oper->row->server_id = BSYNC_SERVER_ID;
		}
		rlist_add_tail_entry(&BSYNC_REMOTE.op_queue, op_status, list);
		bsync_send_data(&BSYNC_REMOTE, elem);
	}
	oper->row->server_id = BSYNC_SERVER_ID;
	oper->row->lsn = oper->gsn;
	ev_tstamp start = ev_now(loop());
	oper->status = bsync_op_status_yield;
	while (2 * oper->accepted <= bsync_state.num_hosts) {
		if (ev_now(loop()) - start > bsync_state.operation_timeout ||
			2 * oper->rejected > bsync_state.num_hosts)
		{
			break;
		}
		fiber_yield_timeout(bsync_state.operation_timeout);BSYNC_TRACE
	}
	oper->result = (2 * oper->accepted > bsync_state.num_hosts ? 0 : -1);
	SWITCH_TO_TXN
	if (!proxy) bsync_wait_slow(oper);
}

static void
bsync_proxy_processor()
{BSYNC_TRACE
	struct bsync_operation *oper = (struct bsync_operation *)
		region_alloc(&fiber()->gc, sizeof(struct bsync_operation));
	struct xrow_header *xrow = (struct xrow_header *)
		region_alloc(&fiber()->gc, sizeof(struct xrow_header));
	xrow_header_decode(xrow, bsync_state.iproxy_pos, bsync_state.iproxy_end);
	oper->row = (struct xrow_header *)
		region_alloc(&fiber()->gc, sizeof(struct xrow_header));
	xrow_copy(xrow, oper->row);
	bsync_state.iproxy_pos = NULL;
	bsync_state.iproxy_end = NULL;

	oper->server_id = oper->row->server_id;
	oper->lsn = oper->row->lsn;
	oper->bsync_owner = fiber();
	oper->result = -1;
	oper->txn_owner = NULL;
	bool slave_proxy = (bsync_state.leader_id != bsync_state.local_id);
	if (slave_proxy) {
		BSYNC_LOCAL.gsn = oper->gsn = oper->row->lsn;
	}
	SWITCH_TO_TXN
	fiber_yield();BSYNC_TRACE
	struct bsync_send_elem *send = (struct bsync_send_elem *)
			region_alloc(&fiber()->gc, sizeof(bsync_send_elem));
	send->oper = oper;
	if (slave_proxy) {
		oper->result = wal_write(wal_local_writer, oper->row);
		send->code = (oper->result < 0 ? bsync_mtype_reject :
						 bsync_mtype_submit);
		bsync_send_data(&BSYNC_LEADER, send);
	} else {
		bsync_queue_leader(oper, true);
		uint8_t host_id = oper->server_id - 1;
		send->code = (oper->result < 0 ? bsync_mtype_proxy_reject :
						 bsync_mtype_proxy_submit);
		bsync_send_data(&BSYNC_REMOTE, send);
		bsync_wait_slow(oper);
	}
}

/*
 * Command handlers block
 */

static void
bsync_body(uint8_t host_id, const char** ipos, const char* iend)
{BSYNC_TRACE
	(void)host_id; (void)ipos; (void)iend;
	uint64_t gsn = mp_decode_uint(ipos);
	(void)gsn;
	bsync_state.iproxy_pos = ipos;
	bsync_state.iproxy_end = iend;
	fiber_call(
		bsync_fiber(&bsync_state.bsync_fiber_cache, bsync_process_fiber)
	);
}

static void
bsync_submit(uint8_t host_id, const char** ipos, const char* iend)
{BSYNC_TRACE
	*ipos = iend;
	assert(!rlist_empty(&BSYNC_REMOTE.op_queue));
	bsync_op_status* op_status = rlist_shift_entry(&BSYNC_REMOTE.op_queue,
						struct bsync_op_status, list);
	++op_status->op->accepted;
	if (op_status->op->server_id > 0) {
		tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
		struct bsync_key node_key = { op_status->op->row->body[0].iov_base,
					      op_status->op->row->body[0].iov_len};
		mh_int_t k = mh_strptr_find(BSYNC_REMOTE.active_ops, node_key, NULL);
		assert(k != mh_end(BSYNC_REMOTE.active_ops));
		struct mh_strptr_node_t *node = mh_strptr_node(BSYNC_REMOTE.active_ops, k);
		char ** val = (char **)&node->val;
		--*val;
		if (val == NULL) mh_strptr_del(BSYNC_REMOTE.active_ops, k, NULL);
		tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
	}
	if (op_status->op->status == bsync_op_status_yield)
		fiber_call(op_status->op->bsync_owner);
}

static void
bsync_reject(uint8_t host_id, const char** ipos, const char* iend)
{BSYNC_TRACE
	*ipos = iend;
	bsync_op_status* op_status = rlist_shift_entry(&BSYNC_REMOTE.op_queue,
						struct bsync_op_status, list);
	++op_status->op->rejected;
	if (op_status->op->status == bsync_op_status_yield)
		fiber_call(op_status->op->bsync_owner);
}

static void
bsync_proxy_request(uint8_t host_id, const char** ipos, const char* iend)
{BSYNC_TRACE
	(void)host_id; (void)ipos; (void)iend;
	mp_decode_uint(ipos);
	bsync_state.iproxy_pos = ipos;
	bsync_state.iproxy_end = iend;
	fiber_call(
		bsync_fiber(&bsync_state.bsync_fiber_cache, bsync_process_fiber)
	);
}

static void
bsync_proxy_accept(uint8_t host_id, const char** ipos, const char* iend)
{BSYNC_TRACE
	(void)host_id; (void)ipos; (void)iend;
	uint64_t gsn = mp_decode_uint(ipos);
	struct bsync_operation *op =
		rlist_shift_entry(&bsync_state.accept_queue, bsync_operation, list);
	op->gsn = gsn;
	assert(*ipos == iend);
	fiber_call(op->bsync_owner);
}

static void
bsync_proxy_submit(uint8_t host_id, const char** ipos, const char* iend)
{BSYNC_TRACE
	(void)host_id; (void)ipos; (void)iend;
	uint64_t gsn = mp_decode_uint(ipos);
	struct bsync_operation *op =
		rlist_shift_entry(&bsync_state.submit_queue, bsync_operation, list);
	op->result = 0;
	assert(*ipos == iend && op->gsn == gsn);
	if (op->status == bsync_op_status_wal) fiber_call(op->bsync_owner);
	else op->status = bsync_op_status_submit;
}

static void
bsync_proxy_reject(uint8_t host_id, const char** ipos, const char* iend)
{BSYNC_TRACE
	(void)host_id; (void)ipos; (void)iend;
	uint64_t gsn = mp_decode_uint(ipos);
	struct bsync_operation *op = NULL;
	if (!rlist_empty(&bsync_state.accept_queue)) {
		op = rlist_shift_entry(&bsync_state.accept_queue,
					bsync_operation, list);
	}
	if (!op || op->gsn != gsn) {
		assert(!rlist_empty(&bsync_state.submit_queue));
		op = rlist_shift_entry(&bsync_state.submit_queue,
					bsync_operation, list);
	}
	assert(*ipos == iend && op->gsn == gsn);
	op->result = -1;
	fiber_call(op->bsync_owner);
}

static void
bsync_ping(uint8_t host_id, const char** ipos, const char* iend)
{
	(void)host_id; (void)ipos; (void)iend;
	uint64_t gsn = mp_decode_uint(ipos);
	if (gsn == BSYNC_REMOTE.gsn) return;
	uint8_t max_host = bsync_max_host();
	BSYNC_REMOTE.gsn = gsn;
	if (bsync_state.state == bsync_state_ready) return;
	if (bsync_max_host() != max_host) bsync_connected(host_id);
}

/*
 * Fibers block
 */

static bool
bsync_is_conflict(struct bsync_operation *oper)
{
	if (oper->server_id == 0) return false;
	struct bsync_key node_key = { oper->row->body[0].iov_base,
					oper->row->body[0].iov_len};
	tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
	auto guard = make_scoped_guard([&](){
		tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
	});
	for (int host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		mh_int_t k = mh_strptr_find(BSYNC_REMOTE.active_ops, node_key, NULL);
		if (k != mh_end(BSYNC_REMOTE.active_ops)) {
			return true;
		}
	}
	for (int host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		struct mh_strptr_node_t node;
		node.key = node_key;
		node.val = (void *)1;
		mh_strptr_put(BSYNC_REMOTE.active_ops, &node, NULL, NULL);
	}
	return false;
}

static void
bsync_txn_process_fiber(va_list /* ap */)
{
restart:BSYNC_TRACE
	struct bsync_operation *oper =
		STAILQ_FIRST(&bsync_state.txn_proxy_input);
	STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
	if (bsync_is_conflict(oper)) {
		oper->result = -1;
	} else {
		struct request *req = (struct request *)
			region_alloc(&fiber()->gc, sizeof(struct request));
		request_create(req, oper->row->type);
		request_decode(req, (const char*)oper->row->body[0].iov_base,
				oper->row->body[0].iov_len);
		req->header = oper->row;

		rlist_add_tail_entry(&bsync_state.txn_queue, oper, list);
		box_process(&null_port, req);
	}
	fiber_gc();
	rlist_add_tail_entry(&bsync_state.txn_fiber_cache, fiber(), state);
	fiber_yield();BSYNC_TRACE
	goto restart;
}

static void
bsync_txn_process(ev_loop *loop, ev_async *watcher, int event)
{BSYNC_TRACE
	(void)loop; (void)watcher; (void)event;
	tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
	STAILQ_CONCAT(&bsync_state.txn_proxy_input,
			&bsync_state.txn_proxy_queue);
	tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
	while (!STAILQ_EMPTY(&bsync_state.txn_proxy_input)) {
		struct bsync_operation *elem =
			STAILQ_FIRST(&bsync_state.txn_proxy_input);
		if (elem->txn_owner) {
			STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
			fiber_call(elem->txn_owner);
		} else {
			fiber_call(bsync_fiber(&bsync_state.txn_fiber_cache,
				bsync_txn_process_fiber));
		}
	}
}

static void
bsync_process_fiber(va_list /* ap */)
{
restart:BSYNC_TRACE
	if (bsync_state.iproxy_end) {
		bsync_proxy_processor();
	} else {
		struct bsync_operation *oper =
			STAILQ_FIRST(&bsync_state.bsync_proxy_input);
		STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
		if (oper->bsync_owner) {
			fiber_call(oper->bsync_owner);
		} else {
			oper->lsn = oper->row->lsn = ++bsync_state.lsn;
			oper->bsync_owner = fiber();
			say_debug("start to proceed request %ld", oper->row->lsn);
			if (bsync_state.leader_id == bsync_state.local_id)
				bsync_queue_leader(oper, false);
			else
				bsync_queue_slave(oper);
		}

	}
	fiber_gc();
	rlist_add_tail_entry(&bsync_state.bsync_fiber_cache, fiber(), state);
	fiber_yield();BSYNC_TRACE
	goto restart;
}

static void
bsync_shutdown_fiber(va_list /* ap */) {
	wal_writer_stop(recovery_state);
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		if (BSYNC_REMOTE.fiber_in) fiber_cancel(BSYNC_REMOTE.fiber_in);
		if (BSYNC_REMOTE.fiber_out) fiber_cancel(BSYNC_REMOTE.fiber_out);
	}
	/* TODO : cancel all active bsync process fibers */
	evio_service_stop(&bsync_coio.evio_service);
	ev_break(bsync_loop, 1);
}

static void
bsync_shutdown() {
	fiber_call(fiber_new("bsync_shutdown", bsync_shutdown_fiber));
}

static void
bsync_process(struct ev_loop *loop, struct ev_async *watcher, int event)
{
	(void)loop; (void)watcher; (void)event;
	if (proxy_wal_writer.is_shutdown) {
		bsync_shutdown();
		return;
	}
	if (bsync_state.state != bsync_state_ready)
		return;
	tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
	STAILQ_CONCAT(&bsync_state.bsync_proxy_input,
			&bsync_state.bsync_proxy_queue);
	tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
	while (!STAILQ_EMPTY(&bsync_state.bsync_proxy_input)) {BSYNC_TRACE
		fiber_call(bsync_fiber(&bsync_state.bsync_fiber_cache,
					bsync_process_fiber));
	}
}


/*
 * Leader election block
 */

static void
bsync_connected(uint8_t host_id)
{BSYNC_TRACE
	if (bsync_state.leader_id == bsync_state.local_id) {
		char* pos = bsync_system_out;
		pos = mp_encode_uint(pos, mp_sizeof_uint(bsync_mtype_leader_submit));
		pos = mp_encode_uint(pos, bsync_mtype_leader_submit);
		bsync_index[host_id].buffer_out = bsync_system_out;
		bsync_index[host_id].buffer_out_size = pos - bsync_system_out;
		if (bsync_index[host_id].fiber_out != fiber()) {
			fiber_call(bsync_index[host_id].fiber_out);
		}
		return;
	}
	if (bsync_state.num_connected == bsync_state.num_hosts &&
		bsync_state.state == bsync_state_started)
	{
		bsync_state.state = bsync_state_initial;
	}
	if (2 * bsync_state.num_connected <= bsync_state.num_hosts ||
		bsync_state.state != bsync_state_initial)
	{
		return;
	}
	bsync_state.state = bsync_state_initial;
	uint8_t max_host_id = bsync_max_host();
	if (max_host_id != bsync_state.local_id) return;
	bsync_state.num_accepted = 1;
	bsync_state.state = bsync_state_leader_accept;
	ssize_t msize = mp_sizeof_uint(BSYNC_LOCAL.gsn) +
		mp_sizeof_uint(bsync_mtype_leader_promise);
	char* pos = bsync_system_out;
	pos = mp_encode_uint(pos, msize);
	pos = mp_encode_uint(pos, bsync_mtype_leader_promise);
	pos = mp_encode_uint(pos, BSYNC_LOCAL.gsn);
	for (uint8_t i = 0; i < bsync_state.num_hosts; ++i) {
		if (i == max_host_id) continue;
		bsync_index[i].buffer_out = bsync_system_out;
		bsync_index[i].buffer_out_size = pos - bsync_system_out;
		if (bsync_index[i].fiber_out != fiber()) {
			fiber_call(bsync_index[i].fiber_out);
		}
	}
}

static void
bsync_election(va_list /* ap */)
{BSYNC_TRACE
	fiber_yield_timeout(bsync_state.election_timeout);BSYNC_TRACE
	if (bsync_state.state != bsync_state_started)
		return;
	bsync_state.state = bsync_state_initial;
	if (2 * bsync_state.num_connected <= bsync_state.num_hosts)
		return;
	bsync_connected(BSYNC_MAX_HOSTS);
}

static void
bsync_disconnected(uint8_t host_id)
{BSYNC_TRACE
	--bsync_state.num_connected;
	mh_strptr_clear(BSYNC_REMOTE.active_ops);
	rlist_create(&BSYNC_REMOTE.send_queue);
	rlist_create(&BSYNC_REMOTE.op_queue);
	if (host_id == bsync_state.leader_id) {
		struct bsync_operation *oper;
		rlist_foreach_entry(oper, &bsync_state.submit_queue, list) {
			oper->result = 0;
			if (oper->status == bsync_op_status_wal) {
				fiber_call(oper->txn_owner);
			} else {
				oper->status = bsync_op_status_submit;
			}
		}
		rlist_foreach_entry_reverse(oper, &bsync_state.accept_queue, list) {
			oper->result = -1;
			fiber_call(oper->txn_owner);
		}
		rlist_create(&bsync_state.accept_queue);
		rlist_create(&bsync_state.submit_queue);
	}
	if (2 * bsync_state.num_connected <= bsync_state.num_hosts ||
		host_id == bsync_state.leader_id)
	{
		bsync_state.leader_id = -1;
		bsync_state.state = bsync_state_initial;
		bsync_connected(host_id);
	}
}

static void
bsync_leader_promise(uint8_t host_id, const char** ipos, const char* iend)
{BSYNC_TRACE
	(void)host_id; (void)ipos; (void)iend;
	bsync_index[host_id].gsn = mp_decode_uint(ipos);
	char* pos = bsync_system_out;
	uint8_t max_host_id = bsync_max_host();
	if (host_id != max_host_id || bsync_state.state > bsync_state_initial) {
		ssize_t msize = mp_sizeof_uint(bsync_mtype_leader_reject) +
			mp_sizeof_uint(max_host_id) +
			mp_sizeof_uint(bsync_index[max_host_id].gsn);
		pos = mp_encode_uint(pos, msize);
		pos = mp_encode_uint(pos, bsync_mtype_leader_reject);
		pos = mp_encode_uint(pos, max_host_id);
		pos = mp_encode_uint(pos, bsync_index[max_host_id].gsn);
	} else {
		ssize_t msize = mp_sizeof_uint(bsync_mtype_leader_accept);
		pos = mp_encode_uint(pos, msize);
		pos = mp_encode_uint(pos, bsync_mtype_leader_accept);
		bsync_state.state = bsync_state_leader_accept;
	}
	bsync_index[host_id].buffer_out = bsync_system_out;
	bsync_index[host_id].buffer_out_size = pos - bsync_system_out;
	fiber_call(bsync_index[host_id].fiber_out);
}

static void
bsync_leader_accept(uint8_t host_id, const char** ipos, const char* iend)
{BSYNC_TRACE
	(void)host_id; (void)ipos; (void)iend;
	if (bsync_state.state != bsync_state_leader_accept) return;
	if (2 * ++bsync_state.num_accepted <= bsync_state.num_hosts) return;
	say_info("new leader are %s", BSYNC_LOCAL.source);
	bsync_state.leader_id = bsync_state.local_id;
	char* pos = bsync_system_out;
	pos = mp_encode_uint(pos, mp_sizeof_uint(bsync_mtype_leader_submit));
	pos = mp_encode_uint(pos, bsync_mtype_leader_submit);
	bsync_state.recovery_hosts = 0;
	for (uint8_t i = 0; i < bsync_state.num_hosts; ++i) {
		if (i == bsync_state.local_id || bsync_index[i].connected < 2)
			continue;
		if (bsync_index[i].gsn < BSYNC_LOCAL.gsn)
			bsync_state.recovery_hosts |= (1 << i);
		bsync_index[i].buffer_out = bsync_system_out;
		bsync_index[i].buffer_out_size = pos - bsync_system_out;
		fiber_call(bsync_index[i].fiber_out);
	}
	if (bsync_state.recovery_hosts) {
		bsync_state.state = bsync_state_recovery;
	} else {
		bsync_state.state = bsync_state_ready;
	}
}

static void
bsync_leader_submit(uint8_t host_id, const char** ipos, const char* iend)
{BSYNC_TRACE
	(void)host_id; (void)ipos; (void)iend;
	bsync_state.leader_id = host_id;
	bsync_state.state = bsync_state_ready;
	say_info("new leader are %s", BSYNC_REMOTE.source);
}

static void
bsync_leader_reject(uint8_t host_id, const char** ipos, const char* iend)
{BSYNC_TRACE
	(void)host_id; (void)ipos; (void)iend;
	uint8_t max_id = mp_decode_uint(ipos);
	bsync_index[max_id].gsn = mp_decode_uint(ipos);
	bsync_connected(host_id);
}

/*
 * Network block
 */

typedef void (*bsync_handler_t)(uint8_t host_id,
				const char** ipos, const char* iend);
static bsync_handler_t bsync_handlers[] = {
	0,
	bsync_leader_promise,
	bsync_leader_accept,
	bsync_leader_submit,
	bsync_leader_reject,
	bsync_body,
	bsync_submit,
	bsync_reject,
	bsync_proxy_request,
	bsync_proxy_accept,
	bsync_proxy_submit,
	bsync_proxy_reject,
	bsync_ping
};

static void
bsync_breadn(struct ev_io *coio, uint8_t host_id, struct ibuf *buf, size_t sz) {
	BSYNC_REMOTE.flags |= bsync_host_active_read;
	coio_breadn(coio, buf, sz);
	BSYNC_REMOTE.flags &= !bsync_host_active_read;
}

static void
bsync_incoming(struct ev_io* coio, struct iobuf* iobuf, uint8_t host_id)
{BSYNC_TRACE
	auto coio_guard = make_scoped_guard([&]() {
		iobuf_delete(iobuf);
		BSYNC_REMOTE.fiber_in = NULL;
	});
	struct ibuf *in = &iobuf->in;
	while (!proxy_wal_writer.is_shutdown) {
		/* Read fixed header */
		if (ibuf_size(in) < 1) {
			bsync_breadn(coio, host_id, in, 1);
		}
		/* Read length */
		if (mp_typeof(*in->pos) != MP_UINT) {
			tnt_raise(ClientError, ER_INVALID_MSGPACK,
				  "packet length");
		}
		ssize_t to_read = mp_check_uint(in->pos, in->end);
		if (to_read > 0)
			bsync_breadn(coio, host_id, in, to_read);
		uint32_t len = mp_decode_uint((const char **) &in->pos);
		/* Read header and body */
		to_read = len - ibuf_size(in);
		if (to_read > 0)
			bsync_breadn(coio, host_id, in, to_read);
		/* proceed message */
		const char* iend = (const char *)in->pos + len;
		const char **ipos = (const char **)&in->pos;
		uint32_t type = mp_decode_uint(ipos);
		assert(type < bsync_mtype_count);
		say_debug("receive message from %s, type %s, length %d",
			BSYNC_REMOTE.source, bsync_mtype_name[type], len);
		assert(type < sizeof(bsync_handlers));
		(*bsync_handlers[type])(host_id, ipos, iend);
		/* cleanup buffer */
		iobuf_reset(iobuf);
		fiber_gc();
	}
}

static void
bsync_accept_handler(va_list ap)
{BSYNC_TRACE
	struct ev_io coio = va_arg(ap, struct ev_io);
	struct sockaddr *addr = va_arg(ap, struct sockaddr *);
	socklen_t addrlen = va_arg(ap, socklen_t);
	struct iobuf *iobuf = va_arg(ap, struct iobuf *);
	(void)addr; (void)addrlen;
	coio_read_timeout(&coio, bsync_system_in, BSYNC_SYSBUFFER_SIZE,
			  bsync_state.read_timeout);
	const char* pos = bsync_system_in;
	uint64_t host_id = mp_decode_uint(&pos);
	assert(host_id < bsync_state.num_hosts);
	BSYNC_REMOTE.gsn = mp_decode_uint(&pos);
	BSYNC_REMOTE.fiber_in = fiber();
	say_info("receive incoming connection from %s, gsn=%ld",
		 BSYNC_REMOTE.source, bsync_index[host_id].gsn);
	if (++BSYNC_REMOTE.connected == 1 && BSYNC_REMOTE.fiber_out) {BSYNC_TRACE
		fiber_call(BSYNC_REMOTE.fiber_out);
	}
	if (BSYNC_REMOTE.connected == 2) {
		++bsync_state.num_connected;
		bsync_connected(host_id);
	}
	try {
		bsync_incoming(&coio, iobuf, host_id);
	} catch(...) {
		if (--BSYNC_REMOTE.connected == 1) {BSYNC_TRACE
			bsync_disconnected(host_id);
			if (BSYNC_REMOTE.fiber_out) {BSYNC_TRACE
				fiber_call(BSYNC_REMOTE.fiber_out);
			}
		}
		throw;
	}
}

static int
encode_request(struct bsync_send_elem* elem, struct iovec* iov) {
	int iovcnt = 1;
	ssize_t bsize = mp_sizeof_uint(elem->code) +
			mp_sizeof_uint(elem->oper->gsn);
	if (elem->code == bsync_mtype_body ||
		elem->code == bsync_mtype_proxy_request)
	{
		iovcnt += xrow_header_encode(elem->oper->row, iov + 1);
		for (int i = 1; i < iovcnt; ++i) {
			bsize += iov[i].iov_len;
		}
	}
	iov[0].iov_len = mp_sizeof_uint(bsize) + mp_sizeof_uint(elem->code) +
			 mp_sizeof_uint(elem->oper->gsn);
	iov[0].iov_base = region_alloc(&fiber()->gc, iov[0].iov_len);
	char* pos = (char*)iov[0].iov_base;
	pos = mp_encode_uint(pos, bsize);
	pos = mp_encode_uint(pos, elem->code);
	pos = mp_encode_uint(pos, elem->oper->gsn);
	return iovcnt;
}

static void
bsync_send(struct ev_io* coio, uint8_t host_id)
{
	if (bsync_state.state == bsync_state_ready &&
		!rlist_empty(&BSYNC_REMOTE.send_queue)) {
		while(!rlist_empty(&BSYNC_REMOTE.send_queue)) {
			struct bsync_send_elem* elem =
				rlist_shift_entry(&BSYNC_REMOTE.send_queue,
					struct bsync_send_elem, state);
			say_debug("send to %s message with type %s",
				BSYNC_REMOTE.source, bsync_mtype_name[elem->code]);
			struct iovec iov[XROW_IOVMAX];
			int iovcnt = encode_request(elem, iov);
			BSYNC_REMOTE.flags |= bsync_host_active_write;
			coio_writev(coio, iov, iovcnt, -1);
			BSYNC_REMOTE.flags &= !bsync_host_active_write;
		}
	} else {
		if (BSYNC_REMOTE.buffer_out == NULL) {
			/* send ping message */
			char* pos = bsync_system_out;
			ssize_t msize = mp_sizeof_uint(bsync_mtype_ping) +
				mp_sizeof_uint(BSYNC_LOCAL.gsn);
			pos = mp_encode_uint(pos, msize);
			pos = mp_encode_uint(pos, bsync_mtype_ping);
			pos = mp_encode_uint(pos, BSYNC_LOCAL.gsn);
			bsync_index[host_id].buffer_out = bsync_system_out;
			bsync_index[host_id].buffer_out_size =
				pos - bsync_system_out;
		}
		const void* buffer_out = BSYNC_REMOTE.buffer_out;
		ssize_t send_size = bsync_index[host_id].buffer_out_size;
		bsync_index[host_id].buffer_out = NULL;
		BSYNC_REMOTE.flags |= bsync_host_active_write;
		ssize_t ssize = coio_write_timeout(coio, buffer_out,
			send_size, bsync_state.write_timeout);
		BSYNC_REMOTE.flags &= !bsync_host_active_write;
		if (ssize < send_size) {
			tnt_raise(SocketError, coio->fd, "timeout");
		}
	}
}

static void
bsync_out_fiber(va_list ap)
{BSYNC_TRACE
	uint8_t host_id = *va_arg(ap, uint8_t*);
	say_info("send outgoing connection to %s", BSYNC_REMOTE.source);
	bool connected = false;
	BSYNC_REMOTE.fiber_out = fiber();
	while (!proxy_wal_writer.is_shutdown) try {BSYNC_TRACE
		connected = false;
		struct ev_io coio;
		coio_init(&coio);
		struct iobuf *iobuf = iobuf_new(BSYNC_REMOTE.source);
		auto coio_guard = make_scoped_guard([&] {
			evio_close(loop(), &coio);
			iobuf_delete(iobuf);
		});
		int r = coio_connect_timeout(&coio, BSYNC_REMOTE.uri.host,
				BSYNC_REMOTE.uri.service, 0, 0,
				bsync_state.connect_timeout,
				BSYNC_REMOTE.uri.host_hint);
		if (r == -1) {
			say_warn("connection timeout to %s, wait %f",
				 BSYNC_REMOTE.source, bsync_state.reconnect_timeout);
			fiber_yield_timeout(bsync_state.reconnect_timeout);BSYNC_TRACE
			continue;
		}
		BSYNC_TRACE
		char* pos = bsync_system_out;
		pos = mp_encode_uint(pos, bsync_state.local_id);
		pos = mp_encode_uint(pos, BSYNC_LOCAL.gsn);
		BSYNC_REMOTE.flags |= bsync_host_active_write;
		coio_write_timeout(&coio, bsync_system_out, BSYNC_SYSBUFFER_SIZE,
				   bsync_state.write_timeout);
		BSYNC_REMOTE.flags &= !bsync_host_active_write;
		connected = true;
		if (++BSYNC_REMOTE.connected == 2) {
			++bsync_state.num_connected;
			bsync_connected(host_id);
		}
		while(!proxy_wal_writer.is_shutdown) {
			bsync_send(&coio, host_id);
			fiber_gc();
			fiber_yield_timeout(bsync_state.ping_timeout);
		}
		BSYNC_REMOTE.fiber_out = NULL;
	} catch (...) {
		BSYNC_TRACE
		BSYNC_REMOTE.flags &= !bsync_host_active_write;
		if (connected && --bsync_index[host_id].connected == 1) {
			bsync_disconnected(host_id);
		}
		if (BSYNC_REMOTE.fiber_in) fiber_cancel(BSYNC_REMOTE.fiber_in);
		fiber_yield_timeout(bsync_state.reconnect_timeout);BSYNC_TRACE
	}
}

/*
 * System block:
 * 1. initialize local variables;
 * 2. read cfg;
 * 3. start/stop cord and event loop
 */

static void
bsync_writer_child()
{
  log_io_atfork(&recovery->current_wal);
  if (proxy_wal_writer.batch) {
    free(proxy_wal_writer.batch);
    proxy_wal_writer.batch = NULL;
  }
  /*
   * Make sure that atexit() handlers in the child do
   * not try to stop the non-existent thread.
   * The writer is not used in the child.
   */
  recovery->writer = NULL;
}

static void
bsync_writer_init_once()
{
  (void) tt_pthread_atfork(NULL, NULL, bsync_writer_child);
}

static void
bsync_cfg_push_host(uint8_t host_id, const char *ibegin,
		    const char* iend, const char* localhost)
{
	assert(iend > ibegin);
	memcpy(BSYNC_REMOTE.source, ibegin, iend - ibegin);
	BSYNC_REMOTE.source[iend - ibegin] = 0;
	BSYNC_REMOTE.fiber_in = NULL;
	BSYNC_REMOTE.fiber_out = NULL;
	uri_parse(&BSYNC_REMOTE.uri, BSYNC_REMOTE.source);
	if (strcmp(BSYNC_REMOTE.source, localhost) == 0) {
		bsync_state.local_id = host_id;
	}
	BSYNC_REMOTE.flags = 0;
	rlist_create(&BSYNC_REMOTE.op_queue);
	rlist_create(&BSYNC_REMOTE.send_queue);
	BSYNC_REMOTE.active_ops = mh_strptr_new();
	if (BSYNC_REMOTE.active_ops == NULL)
		panic("out of memory");
}

static void
bsync_cfg_read()
{
	bsync_state.leader_id = -1;
	bsync_state.num_connected = 1;
	bsync_state.state = bsync_state_started;
	bsync_state.local_id = BSYNC_MAX_HOSTS;

	bsync_state.read_timeout = cfg_getd("bsync_read_timeout");
	bsync_state.write_timeout = cfg_getd("bsync_write_timeout");
	bsync_state.connect_timeout = cfg_getd("bsync_connect_timeout");
	bsync_state.reconnect_timeout = cfg_getd("bsync_reconnect_timeout");
	bsync_state.operation_timeout = cfg_getd("bsync_operation_timeout");
	bsync_state.ping_timeout = cfg_getd("bsync_ping_timeout");
	bsync_state.election_timeout = cfg_getd("bsync_election_timeout");
	bsync_state.slow_host_timeout = cfg_getd("bsync_slow_host_timeout");

	const char* hosts = cfg_gets("bsync_replica");
	const char* localhost = cfg_gets("bsync_local");
	if (hosts == NULL) {
		tnt_raise(ClientError, ER_CFG,
			  "bsync.replica: expected host:port[;host_port]*");
	}
	const char* i_begin = hosts;
	uint8_t host_id = 0;
	for (const char* i_cur = i_begin; *i_cur; ++i_cur) {
		if (*i_cur != ';') continue;
		bsync_cfg_push_host(host_id++, i_begin, i_cur, localhost);
		i_begin = i_cur + 1;
	}
	bsync_cfg_push_host(host_id++, i_begin, hosts + strlen(hosts), localhost);
	if (bsync_state.local_id == BSYNC_MAX_HOSTS) {
		tnt_raise(ClientError, ER_CFG,
			  "bsync.local not found in bsync.replica");
	}
	bsync_state.num_hosts = host_id;
	BSYNC_LOCAL.connected = 2;
	BSYNC_LOCAL.gsn = 1;
}

static void
bsync_init_state(const struct vclock* vclock)
{
	if (vclock->lsn[BSYNC_SERVER_ID] == -1)
		BSYNC_LOCAL.gsn = 0;
	else
		BSYNC_LOCAL.gsn = vclock->lsn[BSYNC_SERVER_ID];
}

void
bsync_init(wal_writer* initial, struct vclock *vclock)
{BSYNC_TRACE
	assert (initial != NULL);
	if (cfg_geti("bsync_enable") <= 0) {
		say_info("bsync_enable=%d\n", cfg_geti("bsync_enable"));
		return;
	}
	wal_local_writer = initial;
	bsync_state.iproxy_end = NULL;
	bsync_state.iproxy_pos = NULL;

	assert(! proxy_wal_writer.is_shutdown);
	assert(STAILQ_EMPTY(&proxy_wal_writer.input));
	assert(STAILQ_EMPTY(&proxy_wal_writer.commit));

	/* I. Initialize the state. */
	pthread_mutexattr_t errorcheck;

	(void) tt_pthread_mutexattr_init(&errorcheck);
#ifndef NDEBUG
	(void) tt_pthread_mutexattr_settype(&errorcheck, PTHREAD_MUTEX_ERRORCHECK);
#endif
	/* Initialize queue lock mutex. */
	(void) tt_pthread_mutex_init(&proxy_wal_writer.mutex, &errorcheck);
	(void) tt_pthread_mutexattr_destroy(&errorcheck);

	(void) tt_pthread_cond_init(&proxy_wal_writer.cond, NULL);

	STAILQ_INIT(&proxy_wal_writer.input);
	STAILQ_INIT(&proxy_wal_writer.commit);
	STAILQ_INIT(&bsync_state.txn_proxy_input);
	STAILQ_INIT(&bsync_state.txn_proxy_queue);
	STAILQ_INIT(&bsync_state.bsync_proxy_input);
	STAILQ_INIT(&bsync_state.bsync_proxy_queue);
	rlist_create(&bsync_state.txn_fiber_cache);
	rlist_create(&bsync_state.bsync_fiber_cache);
	rlist_create(&bsync_state.accept_queue);
	rlist_create(&bsync_state.submit_queue);
	rlist_create(&bsync_state.txn_queue);
	rlist_create(&bsync_state.execute_queue);

	ev_async_init(&txn_process_event, bsync_txn_process);
	ev_async_init(&bsync_process_event, bsync_process);

	txn_loop = loop();

	(void) tt_pthread_once(&bsync_writer_once, bsync_writer_init_once);

	proxy_wal_writer.batch = fio_batch_alloc(sysconf(_SC_IOV_MAX));

	if (proxy_wal_writer.batch == NULL)
		panic_syserror("fio_batch_alloc");

	/* Create and fill writer->cluster hash */
	vclock_create(&proxy_wal_writer.vclock);
	vclock_copy(&proxy_wal_writer.vclock, vclock);

	ev_async_start(loop(), &txn_process_event);

	/* II. Start the thread. */
	bsync_cfg_read();
	bsync_init_state(vclock);
	tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
	if (cord_start(&proxy_wal_writer.cord, "bsync", bsync_thread, NULL)) {
		wal_writer_destroy(&proxy_wal_writer);
		wal_local_writer = NULL;
		return;
	}
	tt_pthread_cond_wait(&proxy_wal_writer.cond, &proxy_wal_writer.mutex);
	tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
	wal_local_writer->txn_loop = bsync_loop;
}

static void*
bsync_thread(void*)
{BSYNC_TRACE
	tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
	coio_service_init(&bsync_coio, "bsync",
		BSYNC_LOCAL.source, bsync_accept_handler, NULL);
	evio_service_start(&bsync_coio.evio_service);
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		if (host_id == bsync_state.local_id) continue;
		bsync_index[host_id].connected = 0;
		fiber_call(
			fiber_new(BSYNC_REMOTE.source, bsync_out_fiber),
			&host_id);
	}
	fiber_call(fiber_new("bsync initial election", bsync_election));
	bsync_loop = loop();
	ev_async_start(bsync_loop, &bsync_process_event);
	tt_pthread_cond_signal(&proxy_wal_writer.cond);
	tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
	try {
		ev_run(loop(), 0);
	} catch (...) {
		say_crit("bsync_thread found unhandled exception");
		throw;
	}
	ev_async_stop(bsync_loop, &bsync_process_event);
	say_info("bsync stopped");
	return NULL;
}

void
bsync_writer_stop(struct recovery_state *r)
{BSYNC_TRACE
	if (wal_local_writer == NULL) {
		wal_writer_stop(r);
		return;
	}
	recovery_state = r;
	proxy_wal_writer.is_shutdown = true;
	tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
	ev_async_send(bsync_loop, &bsync_process_event);
	tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
/*	TODO: temporary disable, uncomment after move network managment from
 * 	bsync to tarantool iproto (main thread)
	if (cord_join(&proxy_wal_writer.cord)) {
		panic_syserror("BSYNC writer: thread join failed");
	}
*/	ev_async_stop(txn_loop, &txn_process_event);
	wal_writer_destroy(&proxy_wal_writer);
	wal_local_writer = NULL;
	rlist_del(&bsync_state.accept_queue);
	rlist_del(&bsync_state.submit_queue);
	rlist_del(&bsync_state.txn_queue);
	rlist_del(&bsync_state.execute_queue);
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		rlist_del(&BSYNC_REMOTE.send_queue);
		rlist_del(&BSYNC_REMOTE.op_queue);
		if (BSYNC_REMOTE.active_ops)
			mh_strptr_delete(BSYNC_REMOTE.active_ops);
	}
}
