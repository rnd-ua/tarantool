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
#define BSYNC_MAX_HOSTS 16

static void* bsync_thread(void*);
static struct coio_service bsync_coio;
static struct wal_writer* wal_local_writer = NULL;
static struct wal_writer proxy_wal_writer;
static pthread_once_t bsync_writer_once = PTHREAD_ONCE_INIT;
static ev_async local_write_event;

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
} bsync_state;

#define BSYNC_TRACE say_debug("%s:%d current state: nconnected=%d, state=%d, naccepted=%d\n", \
	__PRETTY_FUNCTION__, __LINE__, bsync_state.num_connected, bsync_state.state, bsync_state.num_accepted);

#define BSYNC_LOCAL bsync_index[bsync_state.local_id]
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
	bsync_mtype_ping = 10,
	bsync_mtype_count = 11
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
	"bsync_promise",
	"bsync_leader_accept",
	"bsync_leader_submit",
	"bsync_leader_reject",
	"bsync_body",
	"bsync_submit",
	"bsync_reject",
	"bsync_proxy_request",
	"bsync_proxy_submit",
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
bsync_schedule(ev_loop * /* loop */, ev_async *watcher, int /* event */)
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
  /*
   * Perform a cascading abort of all transactions which
   * depend on the transaction which failed to get written
   * to the write ahead log. Abort transactions
   * in reverse order, performing a playback of the
   * in-memory database state.
   */
  STAILQ_REVERSE(&rollback, wal_write_request, wal_fifo_entry);
}

static void
bsync_local_write(ev_loop *loop, ev_async *watcher, int event)
{
	(void)loop; (void)watcher; (void)event;
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

	ev_async_init(&proxy_wal_writer.write_event, bsync_schedule);
	ev_async_init(&local_write_event, bsync_local_write);
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
	ev_async_start(proxy_wal_writer.txn_loop, &local_write_event);

	/* II. Start the thread. */
	bsync_cfg_read();
	bsync_init_state(vclock);
	if (cord_start(&proxy_wal_writer.cord, "bsync", bsync_thread, NULL)) {
		wal_writer_destroy(&proxy_wal_writer);
		return 0;
	}
	return &proxy_wal_writer;
}

int
bsync_write(struct recovery_state *r, struct xrow_header *row)
{
	if (wal_local_writer == NULL) {
		return raft_write(r, row);
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
	if (2 * bsync_state.num_connected <= bsync_state.num_hosts ||
		bsync_state.state > bsync_state_initial)
	{
		return;
	}
	if (bsync_state.state > bsync_state_initial) return;
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
bsync_leader_promise(uint8_t host_id, const char** ipos)
{BSYNC_TRACE
	(void)host_id; (void)ipos;
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
bsync_leader_accept(uint8_t host_id, const char** ipos)
{BSYNC_TRACE
	(void)host_id; (void)ipos;
	if (bsync_state.state != bsync_state_leader_accept) return;
	if (2 * ++bsync_state.num_accepted <= bsync_state.num_hosts) return;
	say_info("new leader are %s", BSYNC_LOCAL.source);
	bsync_state.state = bsync_state_ready;
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
bsync_leader_submit(uint8_t host_id, const char** ipos)
{BSYNC_TRACE
	(void)host_id; (void)ipos;
	bsync_state.leader_id = host_id;
	bsync_state.state = bsync_state_ready;
	say_info("new leader are %s:%s",
		 BSYNC_LOCAL.uri.host, BSYNC_LOCAL.uri.service);
}

static void
bsync_leader_reject(uint8_t host_id, const char** ipos)
{BSYNC_TRACE
	(void)host_id; (void)ipos;
	uint8_t max_id = mp_decode_uint(ipos);
	bsync_index[max_id].gsn = mp_decode_uint(ipos);
	bsync_connected(host_id);
}

static void
bsync_body(uint8_t host_id, const char** ipos)
{BSYNC_TRACE
	(void)host_id; (void)ipos;
}

static void
bsync_submit(uint8_t host_id, const char** ipos)
{BSYNC_TRACE
	(void)host_id; (void)ipos;
}

static void
bsync_reject(uint8_t host_id, const char** ipos)
{BSYNC_TRACE
	(void)host_id; (void)ipos;
}

static void
bsync_proxy_request(uint8_t host_id, const char** ipos)
{BSYNC_TRACE
	(void)host_id; (void)ipos;
}

static void
bsync_proxy_submit(uint8_t host_id, const char** ipos)
{BSYNC_TRACE
	(void)host_id; (void)ipos;
}

static void
bsync_ping(uint8_t host_id, const char** ipos)
{BSYNC_TRACE
	(void)host_id; (void)ipos;
}

typedef void (*bsync_handler_t)(uint8_t host_id, const char** ipos);
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
		const char **ipos = (const char **)&in->pos;
		uint32_t type = mp_decode_uint(ipos);
		say_debug("receive message from %s, type %s, length %d",
			BSYNC_REMOTE.source, bsync_mtype_name[type], len);
		assert(type < sizeof(bsync_handlers));
		(*bsync_handlers[type])(host_id, ipos);
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
	say_info("receive incoming connection from %s:%s, gsn=%ld",
		 BSYNC_LOCAL.uri.host, BSYNC_LOCAL.uri.service,
		 bsync_index[host_id].gsn);
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
		while(true) {BSYNC_TRACE
			if (BSYNC_REMOTE.buffer_out == NULL) {
				char* pos = bsync_system_out;
				pos = mp_encode_uint(pos, mp_sizeof_uint(bsync_mtype_ping));
				pos = mp_encode_uint(pos, bsync_mtype_ping);
				bsync_index[host_id].buffer_out = bsync_system_out;
				bsync_index[host_id].buffer_out_size = pos - bsync_system_out;
			}
			const void* buffer_out = BSYNC_REMOTE.buffer_out;
			ssize_t send_size = bsync_index[host_id].buffer_out_size;
			bsync_index[host_id].buffer_out = NULL;
			ssize_t ssize = coio_write_timeout(&coio, buffer_out,
				send_size, bsync_state.write_timeout);
			if (ssize < send_size) {BSYNC_TRACE
				tnt_raise(SocketError, coio.fd, "timeout");
			}
			BSYNC_TRACE
			fiber_yield_timeout(1); /* wait data for sending */
		}
	} catch (...) {BSYNC_TRACE
		if (connected && --bsync_index[host_id].connected == 1) {
			bsync_disconnected(host_id);
		}
		BSYNC_TRACE
		fiber_yield_timeout(bsync_state.reconnect_timeout);
		BSYNC_TRACE
	}
}

static void*
bsync_thread(void*)
{
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
	ev_run(loop(), 0);
	say_info("bsync stopped");
	return NULL;
}

void
bsync_writer_stop(struct recovery_state *r)
{
	if (wal_local_writer != NULL) {
		(void) tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
		proxy_wal_writer.is_shutdown= true;
		(void) tt_pthread_cond_signal(&proxy_wal_writer.cond);
		(void) tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
//		if (cord_join(&proxy_wal_writer.cord))
//			panic_syserror("RAFT writer: thread join failed");
		ev_async_stop(proxy_wal_writer.txn_loop, &proxy_wal_writer.write_event);
		ev_async_stop(proxy_wal_writer.txn_loop, &local_write_event);
		wal_writer_destroy(&proxy_wal_writer);
		r->writer = wal_local_writer;
		wal_local_writer = NULL;
	}
	raft_writer_stop(r);
}
