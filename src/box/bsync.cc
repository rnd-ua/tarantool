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

#define BSYNC_SYSBUFFER_SIZE 64
#define BSYNC_MAX_HOSTS 14

static void* bsync_thread(void*);
static void bsync_process(struct ev_loop *loop,
			struct ev_async *watcher, int event);
static void bsync_process_fiber(va_list ap);

static void bsync_commit(struct ev_loop *loop,
			struct ev_async *watcher, int event);

static struct coio_service bsync_coio;
static struct wal_writer* wal_local_writer = NULL;
static struct wal_writer proxy_wal_writer;
static pthread_once_t bsync_writer_once = PTHREAD_ONCE_INIT;
static struct ev_async txn_process_event;

static struct ev_async bsync_process_event;
static struct ev_async bsync_commit_event;
static struct ev_loop* writer_loop;

struct bsync_operation {
	uint64_t gsn;
	uint64_t lsn;
	uint8_t server_id;
	uint8_t accepted;
	uint8_t rejected;
	int result;
	struct fiber* owner;

	struct rlist state;
};

struct bsync_send_elem {
	uint8_t code;
	struct xrow_header* row;

	struct rlist state;
};

struct bsync_proceed_elem {
	struct request* request;

	STAILQ_ENTRY(bsync_proceed_elem) bsync_fifo_entry;
};
STAILQ_HEAD(bsync_fifo, bsync_proceed_elem);

struct bsync_host_data {
	uint8_t connected;
	uint64_t gsn;
	char source[REMOTE_SOURCE_MAXLEN];
	struct uri uri;
	struct fiber* fiber_out;
	struct fiber* fiber_in;
	struct io_buf* buffer;
	const void* buffer_out;
	ssize_t buffer_out_size;

	struct rlist op_queue;
	struct rlist send_queue;
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

	struct rlist txn_fiber_cache;
	struct rlist bsync_fiber_cache;

	struct wal_fifo bsync_queue;

	struct bsync_fifo txn_proxy_queue;
	struct bsync_fifo txn_proxy_input;

	struct bsync_fifo bsync_proxy_queue;
	struct bsync_fifo bsync_proxy_input;
} bsync_state;

#define BSYNC_TRACE say_debug("%s:%d current state: nconnected=%d, state=%d, naccepted=%d", \
	__PRETTY_FUNCTION__, __LINE__, bsync_state.num_connected, bsync_state.state, bsync_state.num_accepted);

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
	bsync_mtype_proxy_submit = 9,
	bsync_mtype_proxy_reject = 10,
	bsync_mtype_ping = 11,
	bsync_mtype_count = 12
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
	"bsync_proxy_submit",
	"bsync_proxy_reject",
	"bsync_ping"
};

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
	uri_parse(&BSYNC_REMOTE.uri, BSYNC_REMOTE.source);
	if (strcmp(BSYNC_REMOTE.source, localhost) == 0) {
		bsync_state.local_id = host_id;
	}
	rlist_create(&BSYNC_REMOTE.op_queue);
	rlist_create(&BSYNC_REMOTE.send_queue);
}

static struct fiber*
bsync_fiber(struct rlist *lst, void (*f) (va_list))
{
	if (! rlist_empty(lst))
		return rlist_shift_entry(lst, struct fiber, state);
	else
		return fiber_new("bsync_proc", f);
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
bsync_txn_commit(ev_loop * /* loop */, ev_async *watcher, int /* event */)
{
	struct wal_writer *writer = (struct wal_writer *) watcher->data;
	struct wal_fifo commit = STAILQ_HEAD_INITIALIZER(commit);
	struct wal_fifo rollback = STAILQ_HEAD_INITIALIZER(rollback);

	(void) tt_pthread_mutex_lock(&writer->mutex);
	STAILQ_CONCAT(&commit, &writer->commit);

	if (writer->is_rollback) {
		STAILQ_CONCAT(&rollback, &writer->input);
		writer->is_rollback = false;
	}
	(void) tt_pthread_mutex_unlock(&writer->mutex);
	struct wal_write_request *req, *tmp;
	STAILQ_FOREACH_SAFE(req, &commit, wal_fifo_entry, tmp)
		fiber_call(req->fiber);

	/*
	 * Perform a cascading abort of all transactions which
	 * depend on the transaction which failed to get written
	 * to the write ahead log. Abort transactions
	 * in reverse order, performing a playback of the
	 * in-memory database state.
	 */
	STAILQ_REVERSE(&rollback, wal_write_request, wal_fifo_entry);
	STAILQ_FOREACH_SAFE(req, &rollback, wal_fifo_entry, tmp)
		fiber_call(req->fiber);
}

static void
bsync_txn_process_fiber(va_list /* ap */)
{BSYNC_TRACE
	struct bsync_proceed_elem *elem = STAILQ_FIRST(&bsync_state.txn_proxy_input);
	STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, bsync_fifo_entry);
	box_process(&null_port, elem->request);
	fiber_gc();
	rlist_add_tail_entry(&bsync_state.txn_fiber_cache, fiber(), state);
}

static void
bsync_txn_process(ev_loop *loop, ev_async *watcher, int event)
{
	(void)loop; (void)watcher; (void)event;
	tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
	STAILQ_CONCAT(&bsync_state.txn_proxy_input,
			&bsync_state.txn_proxy_queue);
	tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
	while (!STAILQ_EMPTY(&bsync_state.txn_proxy_input)) {
		if (STAILQ_EMPTY(&bsync_state.txn_proxy_input))
			break;
		fiber_call(bsync_fiber(&bsync_state.txn_fiber_cache,
					bsync_txn_process_fiber));
	}
}

static void
bsync_init_state(const struct vclock* vclock)
{
	if (vclock->lsn[BSYNC_SERVER_ID] == -1)
		BSYNC_LOCAL.gsn = 0;
	else
		BSYNC_LOCAL.gsn = vclock->lsn[BSYNC_SERVER_ID];
}

wal_writer*
bsync_init(wal_writer* initial, struct vclock *vclock)
{
	assert (initial != NULL);
	if (cfg_geti("bsync_enable") <= 0) {
		say_info("bsync_enable=%d\n", cfg_geti("bsync_enable"));
		return initial;
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
	STAILQ_INIT(&bsync_state.bsync_queue);
	rlist_create(&bsync_state.txn_fiber_cache);
	rlist_create(&bsync_state.bsync_fiber_cache);

	ev_async_init(&proxy_wal_writer.write_event, bsync_txn_commit);
	ev_async_init(&txn_process_event, bsync_txn_process);
	ev_async_init(&bsync_process_event, bsync_process);
	ev_async_init(&bsync_commit_event, bsync_commit);

	proxy_wal_writer.write_event.data = &proxy_wal_writer;
	proxy_wal_writer.txn_loop = loop();

	(void) tt_pthread_once(&bsync_writer_once, bsync_writer_init_once);

	proxy_wal_writer.batch = fio_batch_alloc(sysconf(_SC_IOV_MAX));

	if (proxy_wal_writer.batch == NULL)
		panic_syserror("fio_batch_alloc");

	/* Create and fill writer->cluster hash */
	vclock_create(&proxy_wal_writer.vclock);
	vclock_copy(&proxy_wal_writer.vclock, vclock);

	ev_async_start(proxy_wal_writer.txn_loop, &proxy_wal_writer.write_event);
	ev_async_start(proxy_wal_writer.txn_loop, &txn_process_event);

	/* II. Start the thread. */
	bsync_cfg_read();
	bsync_init_state(vclock);
	tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
	if (cord_start(&proxy_wal_writer.cord, "bsync", bsync_thread, NULL)) {
		wal_writer_destroy(&proxy_wal_writer);
		return 0;
	}
	tt_pthread_cond_wait(&proxy_wal_writer.cond, &proxy_wal_writer.mutex);
	tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
	return &proxy_wal_writer;
}

int
bsync_write(struct recovery_state *r, struct xrow_header *row)
{
	if (wal_local_writer == NULL) {
		return raft_write(r, row);
	}
	row->tm = ev_now(loop());
	row->sync = 0;
	struct wal_write_request *req = (struct wal_write_request *)
		region_alloc(&fiber()->gc, sizeof(struct wal_write_request));
	req->fiber = fiber();
	req->res = -1;
	req->row = row;
	if (row->server_id == BSYNC_SERVER_ID &&
		bsync_state.leader_id != bsync_state.local_id)
	{
		req->res = wal_write(wal_local_writer, req);
		tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
		struct bsync_operation *op =
			rlist_first_entry(&BSYNC_LEADER.op_queue,
					struct bsync_operation, state);
		assert(op->gsn == row->lsn);
		op->result = req->res;
		ev_async_send(writer_loop, &bsync_commit_event);
		tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
		return req->res;
	} else {
		if (row->server_id == 0)
			row->server_id = bsync_state.local_id;
		(void) tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
		bool was_empty = STAILQ_EMPTY(&proxy_wal_writer.input);
		STAILQ_INSERT_TAIL(&proxy_wal_writer.input, req, wal_fifo_entry);
		if (was_empty) {
			ev_async_send(writer_loop, &bsync_process_event);
		}
		(void) tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
		fiber_yield(); /* Request was inserted. */
		/* req->res is -1 on error */
		if (req->res < 0)
			return -1; /* error */

		if (bsync_state.leader_id == bsync_state.local_id) {
			/* success, send to local wal writer */
			return wal_write(wal_local_writer, req);
		} else {
			if (wal_write(wal_local_writer, req) < 0) {
				/* TODO : inform BSYNC about fail */
				return -1;
			} else {
				/* TODO : inform BSYNC about success */
			}
			fiber_yield(); /* Request was inserted. */
			return req->res;
		}
	}
	return -1;
}

static uint64_t
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
bsync_disconnected(uint8_t host_id) {
	--bsync_state.num_connected;
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

static void
bsync_body(uint8_t host_id, const char** ipos, const char* iend)
{BSYNC_TRACE
	(void)host_id; (void)ipos; (void)iend;
	bsync_state.iproxy_pos = ipos;
	bsync_state.iproxy_end = iend;
	fiber_call(
		bsync_fiber(&bsync_state.bsync_fiber_cache, bsync_process_fiber)
	);
}

static void
bsync_submit(uint8_t host_id, const char** ipos, const char* iend)
{BSYNC_TRACE
	(void)host_id; (void)ipos; (void)iend;
	*ipos = iend;
	tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
	bsync_operation* op = rlist_first_entry(&BSYNC_LOCAL.op_queue,
						struct bsync_operation, state);
	tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
	++op->accepted;
	fiber_call(op->owner);
}

static void
bsync_reject(uint8_t host_id, const char** ipos, const char* iend)
{BSYNC_TRACE
	(void)host_id; (void)ipos; (void)iend;
	*ipos = iend;
	tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
	bsync_operation* op = rlist_first_entry(&BSYNC_LOCAL.op_queue,
						struct bsync_operation, state);
	tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
	++op->rejected;
	fiber_call(op->owner);
}

static void
bsync_proxy_request(uint8_t host_id, const char** ipos, const char* iend)
{BSYNC_TRACE
	(void)host_id; (void)ipos; (void)iend;
	bsync_state.iproxy_pos = ipos;
	bsync_state.iproxy_end = iend;
	fiber_call(
		bsync_fiber(&bsync_state.bsync_fiber_cache, bsync_process_fiber)
	);
}

static void
bsync_proxy_submit(uint8_t host_id, const char** ipos, const char* iend)
{BSYNC_TRACE
	(void)host_id; (void)ipos; (void)iend;
}

static void
bsync_proxy_reject(uint8_t host_id, const char** ipos, const char* iend)
{BSYNC_TRACE
	(void)host_id; (void)ipos; (void)iend;
}

static void
bsync_ping(uint8_t host_id, const char** ipos, const char* iend)
{
	(void)host_id; (void)ipos; (void)iend;
}

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
	bsync_proxy_submit,
	bsync_proxy_reject,
	bsync_ping
};

static void
bsync_incoming(struct ev_io* coio, struct iobuf* iobuf, uint8_t host_id) {
	auto coio_guard = make_scoped_guard([&]() {
		iobuf_delete(iobuf);
	});
	struct ibuf *in = &iobuf->in;
	while (true) {
		/* Read fixed header */
		if (ibuf_size(in) < 1)
			coio_breadn(coio, in, 1);
		/* Read length */
		if (mp_typeof(*in->pos) != MP_UINT) {
			tnt_raise(ClientError, ER_INVALID_MSGPACK,
				  "packet length");
		}
		ssize_t to_read = mp_check_uint(in->pos, in->end);
		if (to_read > 0)
			coio_breadn(coio, in, to_read);
		uint32_t len = mp_decode_uint((const char **) &in->pos);
		/* Read header and body */
		to_read = len - ibuf_size(in);
		if (to_read > 0)
			coio_breadn(coio, in, to_read);
		/* proceed message */
		const char* iend = (const char *)in->pos + len;
		const char **ipos = (const char **)&in->pos;
		uint32_t type = mp_decode_uint(ipos);
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
bsync_handler(va_list ap)
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
	bsync_index[host_id].gsn = mp_decode_uint(&pos);
	bsync_index[host_id].fiber_in = fiber();
	say_info("receive incoming connection from %s, gsn=%ld",
		 BSYNC_REMOTE.source, bsync_index[host_id].gsn);
	if (++bsync_index[host_id].connected == 1) {
		fiber_call(bsync_index[host_id].fiber_out);
	}
	if (bsync_index[host_id].connected == 2) {
		++bsync_state.num_connected;
		bsync_connected(host_id);
	}
	try {
		bsync_incoming(&coio, iobuf, host_id);
	} catch(...) {
		if (--bsync_index[host_id].connected == 1) {BSYNC_TRACE
			bsync_disconnected(host_id);
			fiber_call(bsync_index[host_id].fiber_out);
		}
		throw;
	}
}

static void
bsync_send(struct ev_io* coio, uint8_t host_id)
{
	if (bsync_state.state == bsync_state_ready) {
		while(!rlist_empty(&BSYNC_REMOTE.send_queue)) {
			struct bsync_send_elem* op =
				rlist_shift_entry(&BSYNC_REMOTE.send_queue,
					struct bsync_send_elem, state);
			struct iovec iov[XROW_IOVMAX];
			int iovcnt = xrow_header_encode(op->row, iov + 1) + 1;
			ssize_t bsize = mp_sizeof_uint(op->code);
			for (int i = 1; i < iovcnt; ++i)
				bsize += iov[i].iov_len;
			iov[0].iov_len =
				mp_sizeof_uint(bsize) + mp_sizeof_uint(op->code);
			iov[0].iov_base =
				region_alloc(&fiber()->gc, iov[0].iov_len);
			char* pos = (char*)iov[0].iov_base;
			pos = mp_encode_uint(pos, bsize);
			pos = mp_encode_uint(pos, op->code);
			coio_writev(coio, iov, iovcnt, -1);
		}
	} else {
		if (BSYNC_REMOTE.buffer_out == NULL) {
			/* send ping message */
			char* pos = bsync_system_out;
			pos = mp_encode_uint(pos,
				mp_sizeof_uint(bsync_mtype_ping));
			pos = mp_encode_uint(pos, bsync_mtype_ping);
			bsync_index[host_id].buffer_out = bsync_system_out;
			bsync_index[host_id].buffer_out_size =
				pos - bsync_system_out;
		}
		const void* buffer_out = BSYNC_REMOTE.buffer_out;
		ssize_t send_size = bsync_index[host_id].buffer_out_size;
		bsync_index[host_id].buffer_out = NULL;
		ssize_t ssize = coio_write_timeout(coio, buffer_out,
			send_size, bsync_state.write_timeout);
		if (ssize < send_size) {
			tnt_raise(SocketError, coio->fd, "timeout");
		}
	}
}

static void
bsync_out_fiber(va_list ap)
{BSYNC_TRACE
	uint8_t host_id = *va_arg(ap, uint8_t*);
	BSYNC_REMOTE.fiber_out = fiber();
	say_info("send outgoing connection to %s", BSYNC_REMOTE.source);
	bool connected = false;
	while (true) try {BSYNC_TRACE
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
			say_warn("connection timeout to %s", BSYNC_REMOTE.source);
			continue;
		}
		char* pos = bsync_system_out;
		pos = mp_encode_uint(pos, bsync_state.local_id);
		pos = mp_encode_uint(pos, BSYNC_LOCAL.gsn);
		coio_write_timeout(&coio, bsync_system_out, BSYNC_SYSBUFFER_SIZE,
				   bsync_state.write_timeout);
		connected = true;
		if (++BSYNC_REMOTE.connected == 2) {
			++bsync_state.num_connected;
			bsync_connected(host_id);
		}
		while(true) {
			bsync_send(&coio, host_id);
			fiber_gc();
			fiber_yield_timeout(bsync_state.ping_timeout);
		}
	} catch (...) {
		if (connected && --bsync_index[host_id].connected == 1) {
			bsync_disconnected(host_id);
		}
		fiber_yield_timeout(bsync_state.reconnect_timeout);
	}
}

static void
bsync_commit(struct ev_loop *loop, struct ev_async *watcher, int event)
{BSYNC_TRACE
	(void)loop; (void)watcher; (void)event;
	tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
	bsync_operation* op = rlist_first_entry(&BSYNC_LEADER.op_queue,
						struct bsync_operation, state);
	tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
	fiber_call(op->owner);
}

static void
bsync_proxy_processor()
{
	struct xrow_header *xrow = (struct xrow_header *)
		region_alloc(&fiber()->gc, sizeof(struct xrow_header));
	xrow_header_decode(xrow, bsync_state.iproxy_pos, bsync_state.iproxy_end);
	bsync_state.iproxy_pos = NULL;
	bsync_state.iproxy_end = NULL;

	struct request *req = (struct request *)
		region_alloc(&fiber()->gc, sizeof(struct request));
	request_create(req, xrow->type);
	request_decode(req, (const char*)xrow->body[0].iov_base, xrow->body[0].iov_len);
	req->header = xrow;

	struct bsync_operation *op = (struct bsync_operation *)
		region_alloc(&fiber()->gc, sizeof(struct bsync_operation));
	op->accepted = op->rejected = 0;
	op->lsn = op->gsn = xrow->lsn;
	op->server_id = xrow->server_id;
	op->owner = fiber();
	op->result = -1;

	struct bsync_proceed_elem *elem = (struct bsync_proceed_elem *)
		region_alloc(&fiber()->gc, sizeof(struct bsync_proceed_elem));
	elem->request = req;

	tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
	rlist_add_tail_entry(&BSYNC_LEADER.op_queue, op, state);
	bool was_empty = STAILQ_EMPTY(&bsync_state.txn_proxy_queue);
	STAILQ_INSERT_TAIL(&bsync_state.txn_proxy_queue, elem, bsync_fifo_entry);
	if (was_empty) {
		ev_async_send(proxy_wal_writer.txn_loop, &txn_process_event);
	}
	tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
	fiber_yield(); /* wait for execute operation */

	struct bsync_send_elem *send = (struct bsync_send_elem *)
			region_alloc(&fiber()->gc, sizeof(bsync_send_elem));
	send->row = xrow;
	if (op->server_id == BSYNC_SERVER_ID) {
		send->code = (op->result == -1 ? bsync_mtype_reject :
						 bsync_mtype_submit);
		rlist_add_tail_entry(&BSYNC_LEADER.send_queue, send, state);
		fiber_call(BSYNC_LEADER.fiber_out);
	} else {
		uint8_t host_id = op->server_id;
		send->code = (op->result == -1 ? bsync_mtype_proxy_reject :
						 bsync_mtype_proxy_submit);
		rlist_add_tail_entry(&BSYNC_REMOTE.send_queue, send, state);
		fiber_call(BSYNC_REMOTE.fiber_out);
	}
}

static void
bsync_queue_slave(wal_write_request *wreq, struct bsync_operation *op,
		  struct bsync_send_elem *elem)
{
	elem->row->server_id = op->server_id = bsync_state.local_id;
	elem->code = bsync_mtype_proxy_request;
	tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
	rlist_add_tail_entry(&BSYNC_LOCAL.op_queue, op, state);
	tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
	rlist_add_tail_entry(&BSYNC_LEADER.send_queue, elem, state);
			fiber_call(BSYNC_LEADER.fiber_out);
	fiber_yield();
	(void)wreq;
}

static void
bsync_queue_leader(wal_write_request *wreq, struct bsync_operation *op,
		   struct bsync_send_elem *elem)
{
	wreq->row->lsn = elem->row->lsn = op->gsn = ++BSYNC_LOCAL.gsn;
	wreq->row->server_id = BSYNC_SERVER_ID;
	elem->code = bsync_mtype_body;
	tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
	rlist_add_tail_entry(&BSYNC_LOCAL.op_queue, op, state);
	tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		if (BSYNC_REMOTE.connected < 2 ||
			host_id == bsync_state.local_id)
			continue;
		if (op->server_id == host_id) {
			elem->row->server_id = op->server_id;
			elem->row->lsn = op->lsn;
		} else {
			elem->row->server_id = BSYNC_SERVER_ID;
			elem->row->lsn = op->gsn;
		}
		rlist_add_tail_entry(&BSYNC_REMOTE.send_queue, elem, state);
		fiber_call(BSYNC_REMOTE.fiber_out);
	}
	ev_tstamp start = ev_now(loop());
	while (2 * op->accepted <= bsync_state.num_hosts) {
		if (ev_now(loop()) - start > bsync_state.operation_timeout ||
			2 * op->rejected > bsync_state.num_hosts)
		{
			break;
		}
		fiber_yield_timeout(bsync_state.operation_timeout);
	}
	wreq->res = (2 * op->accepted > bsync_state.num_hosts ? 0 : -1);
	/* fiber_call(wreq->fiber); */
	tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
	STAILQ_INSERT_TAIL(&proxy_wal_writer.commit, wreq, wal_fifo_entry);
	ev_async_send(proxy_wal_writer.txn_loop, &proxy_wal_writer.write_event);
	tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);

	start = ev_now(loop());
	while ((op->accepted + op->rejected) < bsync_state.num_hosts) {
		fiber_yield_timeout(bsync_state.slow_host_timeout);
		if (ev_now(loop()) - start <= bsync_state.slow_host_timeout)
			continue;
		/* disconnect slow nodes */
		break;
	}
	tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
	struct bsync_operation *cop = rlist_shift_entry(&BSYNC_LOCAL.op_queue,
						struct bsync_operation, state);
	tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
	assert(cop == op);
}

static void
bsync_queue_processor(wal_write_request* wreq)
{
	say_debug("start to proceed request %ld", wreq->row->lsn);
	struct bsync_send_elem *elem = (struct bsync_send_elem *)
		region_alloc(&fiber()->gc, sizeof(bsync_send_elem));
	elem->row = (struct xrow_header *)
		region_alloc(&fiber()->gc, sizeof(xrow_header));
	xrow_copy(wreq->row, elem->row);
	struct bsync_operation *op = (struct bsync_operation *)
		region_alloc(&fiber()->gc, sizeof(bsync_operation));
	op->lsn = ++bsync_state.lsn;
	op->owner = fiber();
	op->accepted = 1;
	op->rejected = 0;
	op->result = -1;
	if (bsync_state.leader_id == bsync_state.local_id)
		bsync_queue_leader(wreq, op, elem);
	else
		bsync_queue_slave(wreq, op, elem);
}

static void
bsync_process_fiber(va_list /* ap */)
{BSYNC_TRACE
restart:
	if (bsync_state.iproxy_end) {
		bsync_proxy_processor();
	} else {
		struct wal_write_request* wreq = STAILQ_FIRST(&bsync_state.bsync_queue);
		STAILQ_REMOVE_HEAD(&bsync_state.bsync_queue, wal_fifo_entry);
		bsync_queue_processor(wreq);
	}
	fiber_gc();
	rlist_add_tail_entry(&bsync_state.bsync_fiber_cache, fiber(), state);
	fiber_yield();
	goto restart;
}

static void
bsync_process(struct ev_loop *loop, struct ev_async *watcher, int event)
{BSYNC_TRACE
	(void)loop; (void)watcher; (void)event;

	tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
	STAILQ_CONCAT(&bsync_state.bsync_queue, &proxy_wal_writer.input);
	tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
	while (!STAILQ_EMPTY(&bsync_state.bsync_queue)) {BSYNC_TRACE
		fiber_call(bsync_fiber(&bsync_state.bsync_fiber_cache,
					bsync_process_fiber));
	}
}

static void
bsync_election(va_list /* ap */)
{
	fiber_yield_timeout(bsync_state.election_timeout);
	if (bsync_state.state != bsync_state_started)
		return;
	bsync_state.state = bsync_state_initial;
	if (2 * bsync_state.num_connected <= bsync_state.num_hosts)
		return;
	bsync_connected(BSYNC_MAX_HOSTS);
}

static void*
bsync_thread(void*)
{
	tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
	coio_service_init(&bsync_coio, "bsync",
		BSYNC_LOCAL.source, bsync_handler, NULL);
	evio_service_start(&bsync_coio.evio_service);
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		if (host_id == bsync_state.local_id) continue;
		bsync_index[host_id].connected = 0;
		fiber_call(
			fiber_new(BSYNC_REMOTE.source, bsync_out_fiber),
			&host_id);
	}
	fiber_call(fiber_new("bsync initial election", bsync_election));
	writer_loop = loop();
	ev_async_start(writer_loop, &bsync_process_event);
	ev_async_start(writer_loop, &bsync_commit_event);
	tt_pthread_cond_signal(&proxy_wal_writer.cond);
	tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
	ev_run(loop(), 0);
	say_info("bsync stopped");
	return NULL;
}

void
bsync_writer_stop(struct recovery_state *r)
{
	if (wal_local_writer == NULL) {
		raft_writer_stop(r);
		return;
	}
	(void) tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
	proxy_wal_writer.is_shutdown = true;
	ev_break(writer_loop, 1);
	(void) tt_pthread_cond_signal(&proxy_wal_writer.cond);
	(void) tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
//	if (cord_join(&proxy_wal_writer.cord))
//		panic_syserror("RAFT writer: thread join failed");
	ev_async_stop(proxy_wal_writer.txn_loop, &proxy_wal_writer.write_event);
	ev_async_stop(proxy_wal_writer.txn_loop, &txn_process_event);
	ev_async_stop(writer_loop, &bsync_process_event);
	ev_async_stop(writer_loop, &bsync_commit_event);
	wal_writer_destroy(&proxy_wal_writer);
	r->writer = wal_local_writer;
	wal_local_writer = NULL;
	raft_writer_stop(r);
}
