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
#include "iproto.h"
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdio.h>

#include "iproto_port.h"
#include "tarantool.h"
#include "fiber.h"
#include "say.h"
#include "evio.h"
#include "scoped_guard.h"
#include "memory.h"
#include "msgpuck/msgpuck.h"
#include "session.h"
#include "third_party/base64.h"
#include "xrow.h"
#include "iproto_constants.h"
#include "user_def.h"
#include "authentication.h"
#include "stat.h"
#include "lua/call.h"

static struct mempool iproto_obuf_pool;
#define IPROTO_TRACE say_info("%s:%d", __PRETTY_FUNCTION__, __LINE__);

/* {{{ iproto_request - declaration */

struct iproto_connection;

typedef void (*iproto_request_f)(struct iproto_request *);

enum iproto_request_type {
	iproto_request_connect = 0,
	iproto_request_user = 1,
	iproto_request_shutdown = 2
};

/**
 * A single request from the client. All requests
 * from all clients are queued into a single queue
 * and processed in FIFO order.
 */
struct iproto_request
{
	struct iproto_connection *connection;
	struct iobuf *iobuf;
	struct session *session;
	iproto_request_f process;
	/* Request message code and sync. */
	struct xrow_header header;
	/* Box request, if this is a DML */
	struct request request;
	size_t total_len;

	/* fields for proceed request result in IPROTO context */
	struct obuf_svp response_pos;
	uint8_t type;
	bool result;
	STAILQ_ENTRY(iproto_request) fifo;
};
STAILQ_HEAD(iproto_fifo, iproto_request);

static struct iproto_state_ {
	struct cord cord;
	pthread_cond_t cond;
	pthread_mutex_t mutex;
	ev_async txn_event;
	ev_async iproto_event;

	struct ev_loop* txn_loop;
	struct ev_loop* iproto_loop;

	struct iproto_fifo txn_queue;
	struct iproto_fifo txn_input;
	struct iproto_fifo iproto_queue;
	struct iproto_fifo iproto_input;

	struct rlist fiber_cache;
} iproto_state;

#define SWITCH_TO_TXN { \
	++ireq->iobuf->ref_count; \
	tt_pthread_mutex_lock(&iproto_state.mutex); \
	bool was_empty = STAILQ_EMPTY(&iproto_state.txn_queue); \
	STAILQ_INSERT_TAIL(&iproto_state.txn_queue, ireq, fifo); \
	if (was_empty) \
		ev_async_send(iproto_state.txn_loop, \
			      &iproto_state.txn_event); \
	tt_pthread_mutex_unlock(&iproto_state.mutex); }

static void*
iproto_thread(void *worker_args);

struct mempool iproto_request_pool;

static struct iproto_request *
iproto_request_new(struct iproto_connection *con,
		   iproto_request_f process, uint8_t type);

static void
iproto_process_connect(struct iproto_request *request);

static void
iproto_process_disconnect(struct iproto_request *request);

static void
iproto_process(struct iproto_request *request);

struct IprotoRequestGuard {
	struct iproto_request *ireq;
	IprotoRequestGuard(struct iproto_request *ireq_arg):ireq(ireq_arg) {}
	~IprotoRequestGuard()
	{ if (ireq) mempool_free(&iproto_request_pool, ireq); }
	struct iproto_request *release()
	{ struct iproto_request *tmp = ireq; ireq = NULL; return tmp; }
};

/* }}} */

/* {{{ iproto_queue */

/**
 * Main function of the fiber invoked to handle all outstanding
 * tasks in a queue.
 */
static void
iproto_queue_handler(va_list /* ap */)
{
restart:
	struct iproto_request *ireq = STAILQ_FIRST(&iproto_state.txn_input);
	STAILQ_REMOVE_HEAD(&iproto_state.txn_input, fifo);
	fiber_set_session(fiber(), ireq->session);
	fiber_set_user(fiber(), &ireq->session->credentials);
	ireq->process(ireq);
	ireq->response_pos = obuf_create_svp(&ireq->iobuf->out);
	tt_pthread_mutex_lock(&iproto_state.mutex);
	bool was_empty = STAILQ_EMPTY(&iproto_state.iproto_queue);
	STAILQ_INSERT_TAIL(&iproto_state.iproto_queue, ireq, fifo);
	if (was_empty)
		ev_async_send(iproto_state.iproto_loop,
				&iproto_state.iproto_event);
	tt_pthread_mutex_unlock(&iproto_state.mutex);

	/** Put the current fiber into a queue fiber cache. */
	rlist_add_entry(&iproto_state.fiber_cache, fiber(), state);
	fiber_yield();
	goto restart;
}

/** Create fibers to handle all outstanding tasks. */
static void
iproto_queue_schedule(ev_loop * /* loop */, struct ev_async * /* watcher */,
		      int /* events */)
{
	tt_pthread_mutex_lock(&iproto_state.mutex);
	STAILQ_CONCAT(&iproto_state.txn_input, &iproto_state.txn_queue);
	tt_pthread_mutex_unlock(&iproto_state.mutex);
	struct fiber *f;
	while (!STAILQ_EMPTY(&iproto_state.txn_input)) {
		if (! rlist_empty(&iproto_state.fiber_cache))
			f = rlist_shift_entry(&iproto_state.fiber_cache,
					      struct fiber, state);
		else
			f = fiber_new("iproto", iproto_queue_handler);
		fiber_call(f);
	}
}

/* }}} */

/* {{{ iproto_connection */

/** Context of a single client connection. */
struct iproto_connection
{
	/**
	 * Two rotating buffers for I/O. Input is always read into
	 * iobuf[0]. As soon as iobuf[0] input buffer becomes full,
	 * iobuf[0] is moved to iobuf[1], for flushing. As soon as
	 * all output in iobuf[1].out is sent to the client, iobuf[1]
	 * and iobuf[0] are moved around again.
	 */
	struct iobuf *iobuf[2];
	/*
	 * Size of readahead which is not parsed yet, i.e.
	 * size of a piece of request which is not fully read.
	 * Is always relative to iobuf[0]->in.end. In other words,
	 * iobuf[0]->in.end - parse_size gives the start of the
	 * unparsed request. A size rather than a pointer is used
	 * to be safe in case in->buf is reallocated. Being
	 * relative to in->end, rather than to in->pos is helpful to
	 * make sure ibuf_reserve() or iobuf rotation don't make
	 * the value meaningless.
	 */
	ssize_t parse_size;
	/** Current write position in the output buffer */
	struct obuf_svp write_pos;
	/**
	 * Function of the request processor to handle
	 * a single request.
	 */
	struct ev_io input;
	struct ev_io output;
	/** Logical session. */
	struct session *session;
	uint64_t cookie;
	ev_loop *loop;
	/* Pre-allocated disconnect request. */
	struct iproto_request *disconnect;
};

static struct mempool iproto_connection_pool;

/**
 * A connection is idle when the client is gone
 * and there are no outstanding requests in the request queue.
 * An idle connection can be safely garbage collected.
 * Note: a connection only becomes idle after iproto_connection_close(),
 * which closes the fd.  This is why here the check is for
 * evio_is_active() (false if fd is closed), not ev_is_active()
 * (false if event is not started).
 */
static inline bool
iproto_connection_is_idle(struct iproto_connection *con)
{
	return !evio_is_active(&con->input) &&
		ibuf_size(&con->iobuf[0]->in) == 0 &&
		ibuf_size(&con->iobuf[1]->in) == 0;
}

static void
iproto_connection_on_input(ev_loop * /* loop */, struct ev_io *watcher,
			   int /* revents */);
static void
iproto_connection_on_output(ev_loop * /* loop */, struct ev_io *watcher,
			    int /* revents */);

static int
iproto_flush(struct iobuf *iobuf, int fd, struct obuf_svp *svp);

static struct iproto_connection *
iproto_connection_new(const char *name, int fd, struct sockaddr *addr)
{
	struct iproto_connection *con = (struct iproto_connection *)
		mempool_alloc(&iproto_connection_pool);
	con->input.data = con->output.data = con;
	con->loop = loop();
	ev_io_init(&con->input, iproto_connection_on_input, fd, EV_READ);
	ev_io_init(&con->output, iproto_connection_on_output, fd, EV_WRITE);
	con->iobuf[0] = iobuf_alloc(name);
	con->iobuf[1] = iobuf_alloc(name);
	if (con->iobuf[0]->in.pool == NULL)
		iobuf_ibuf_init(con->iobuf[0], &con->iobuf[0]->pool);
	if (con->iobuf[1]->in.pool == NULL)
		iobuf_ibuf_init(con->iobuf[1], &con->iobuf[1]->pool);
	con->parse_size = 0;
	con->write_pos = obuf_create_svp(&con->iobuf[0]->out);
	con->session = NULL;
	con->cookie = *(uint64_t *) addr;
	/* It may be very awkward to allocate at close. */
	con->disconnect = iproto_request_new(con, iproto_process_disconnect,
					     iproto_request_shutdown);
	return con;
}

/** Recycle a connection. Never throws. */
static inline void
iproto_connection_delete(struct iproto_connection *con)
{
	assert(iproto_connection_is_idle(con));
	assert(!evio_is_active(&con->output));
	if (con->session) {
		if (! rlist_empty(&session_on_disconnect))
			session_run_on_disconnect_triggers(con->session);
		session_destroy(con->session);
	}
}

static inline void
iproto_connection_try_delete(struct iproto_connection *con)
{
	/*
	 * If the con is not idle, it is destroyed
	 * after the last request is handled. Otherwise,
	 * queue a separate request to run on_disconnect()
	 * trigger and destroy the connection.
	 * Sic: the check is mandatory to not destroy a connection
	 * twice.
	 */
	if (iproto_connection_is_idle(con)) {
		struct iproto_request *ireq = con->disconnect;
		con->disconnect = NULL;
		SWITCH_TO_TXN
	}
}

static inline void
iproto_connection_shutdown(struct iproto_connection *con)
{
	ev_io_stop(con->loop, &con->input);
	ev_io_stop(con->loop, &con->output);
	con->input.fd = con->output.fd = -1;
	/*
	 * Discard unparsed data, to recycle the con
	 * as soon as all parsed data is processed.
	 */
	con->iobuf[0]->in.end -= con->parse_size;

	iproto_connection_try_delete(con);
}

static inline void
iproto_connection_close(struct iproto_connection *con)
{
	int fd = con->input.fd;
	iproto_connection_shutdown(con);
	close(fd);
}

static void
iproto_response_schedule(ev_loop * /* loop */, struct ev_async * /* watcher */,
			 int /* events */)
{
	tt_pthread_mutex_lock(&iproto_state.mutex);
	STAILQ_CONCAT(&iproto_state.iproto_input, &iproto_state.iproto_queue);
	tt_pthread_mutex_unlock(&iproto_state.mutex);
	struct iproto_request *ireq = NULL;
	while (!STAILQ_EMPTY(&iproto_state.iproto_input)) {
		ireq = STAILQ_FIRST(&iproto_state.iproto_input);
		STAILQ_REMOVE_HEAD(&iproto_state.iproto_input, fifo);
		IprotoRequestGuard guard(ireq);
		struct iproto_connection *con = ireq->connection;
		--ireq->iobuf->ref_count;
		assert(ireq->iobuf->out_pos.pos <= ireq->response_pos.pos ||
			ireq->iobuf->out_pos.iov_len <= ireq->response_pos.iov_len);
		assert(ireq->response_pos.pos <= ireq->iobuf->out.pos);
		ireq->iobuf->out_pos = ireq->response_pos;
		switch (ireq->type) {
		case iproto_request_shutdown:
			iobuf_ibuf_delete(con->iobuf[0]);
			iobuf_ibuf_delete(con->iobuf[1]);
			iobuf_free(con->iobuf[0]);
			iobuf_free(con->iobuf[1]);
			mempool_free(&iproto_connection_pool, con);
			break;
		case iproto_request_connect:
			if (!ireq->result) {
				try {
					iproto_flush(ireq->iobuf, con->output.fd,
						     &con->write_pos);
				} catch (Exception *e) {
					e->log();
				}
				iproto_connection_close(con);
				break;
			} else {
				/* Handshake OK, start reading input. */
				ev_feed_event(con->loop, &con->input, EV_READ);
			}
		case iproto_request_user:
			if (ireq->header.type == IPROTO_JOIN ||
				ireq->header.type == IPROTO_SUBSCRIBE)
			{
				/* TODO: check requests in `con' queue */
				iproto_connection_shutdown(con);
				break;
			}
			ireq->iobuf->in.pos += ireq->total_len;
			if (evio_is_active(&con->output)) {
				if (! ev_is_active(&con->output))
					ev_feed_event(con->loop,
						&con->output,
						EV_WRITE);
			} else {
				iproto_connection_try_delete(con);
			}
			break;
		}
	}
}

/**
 * If there is no space for reading input, we can do one of the
 * following:
 * - try to get a new iobuf, so that it can fit the request.
 *   Always getting a new input buffer when there is no space
 *   makes the server susceptible to input-flood attacks.
 *   Therefore, at most 2 iobufs are used in a single connection,
 *   one is "open", receiving input, and the  other is closed,
 *   flushing output.
 * - stop input and wait until the client reads piled up output,
 *   so the input buffer can be reused. This complements
 *   the previous strategy. It is only safe to stop input if it
 *   is known that there is output. In this case input event
 *   flow will be resumed when all replies to previous requests
 *   are sent, in iproto_connection_gc_iobuf(). Since there are two
 *   buffers, the input is only stopped when both of them
 *   are fully used up.
 *
 * To make this strategy work, each iobuf in use must fit at
 * least one request. Otherwise, iobuf[1] may end
 * up having no data to flush, while iobuf[0] is too small to
 * fit a big incoming request.
 */
static struct iobuf *
iproto_connection_input_iobuf(struct iproto_connection *con)
{
	struct iobuf *oldbuf = con->iobuf[0];

	ssize_t to_read = 3; /* Smallest possible valid request. */

	/* The type code is checked in iproto_enqueue_batch() */
	if (con->parse_size) {
		const char *pos = oldbuf->in.end - con->parse_size;
		if (mp_check_uint(pos, oldbuf->in.end) <= 0)
			to_read = mp_decode_uint(&pos);
	}

	if (ibuf_unused(&oldbuf->in) >= to_read)
		return oldbuf;

	/** All requests are processed, reuse the buffer. */
	if (ibuf_size(&oldbuf->in) == con->parse_size) {
		ibuf_reserve(&oldbuf->in, to_read);
		return oldbuf;
	}

	if (! iobuf_is_idle(con->iobuf[1])) {
		/*
		 * Wait until the second buffer is flushed
		 * and becomes available for reuse.
		 */
		return NULL;
	}
	struct iobuf *newbuf = con->iobuf[1];

	ibuf_reserve(&newbuf->in, to_read + con->parse_size);
	/*
	 * Discard unparsed data in the old buffer, otherwise it
	 * won't be recycled when all parsed requests are processed.
	 */
	oldbuf->in.end -= con->parse_size;
	/* Move the cached request prefix to the new buffer. */
	memcpy(newbuf->in.pos, oldbuf->in.end, con->parse_size);
	newbuf->in.end += con->parse_size;
	/*
	 * Rotate buffers. Not strictly necessary, but
	 * helps preserve response order.
	 */
	con->iobuf[1] = oldbuf;
	con->iobuf[0] = newbuf;
	return newbuf;
}

/** Enqueue all requests which were read up. */
static inline void
iproto_enqueue_batch(struct iproto_connection *con, struct ibuf *in)
{
	while (true) {
		const char *reqstart = in->end - con->parse_size;
		const char *pos = reqstart;
		/* Read request length. */
		if (mp_typeof(*pos) != MP_UINT) {
			tnt_raise(ClientError, ER_INVALID_MSGPACK,
				  "packet length");
		}
		if (mp_check_uint(pos, in->end) >= 0)
			break;
		uint32_t len = mp_decode_uint(&pos);
		const char *reqend = pos + len;
		if (reqend > in->end)
			break;
		struct iproto_request *ireq =
			iproto_request_new(con, iproto_process,
					   iproto_request_user);
		IprotoRequestGuard guard(ireq);

		xrow_header_decode(&ireq->header, &pos, reqend);
		ireq->total_len = pos - reqstart; /* total request length */

		/*
		 * sic: in case of exception con->parse_size
		 * as well as in->pos must not be advanced, to
		 * stay in sync.
		 */
		if (ireq->header.type >= IPROTO_SELECT &&
		    ireq->header.type <= IPROTO_EVAL) {
			/* Pre-parse request before putting it into the queue */
			if (ireq->header.bodycnt == 0) {
				tnt_raise(ClientError, ER_INVALID_MSGPACK,
					  "request type");
			}
			request_create(&ireq->request, ireq->header.type);
			pos = (const char *) ireq->header.body[0].iov_base;
			request_decode(&ireq->request, pos,
				       ireq->header.body[0].iov_len);
		}
		ireq->request.header = &ireq->header;
		ireq = guard.release();
		switch (ireq->header.type) {
		case IPROTO_JOIN:
		case IPROTO_SUBSCRIBE:
			ev_io_stop(con->loop, &con->input);
			ev_io_stop(con->loop, &con->output);
			break;
		}
		SWITCH_TO_TXN
		/* Request will be discarded in iproto_process_XXX */

		/* Request is parsed */
		con->parse_size -= reqend - reqstart;
		if (con->parse_size == 0)
			break;
	}
}

static void
iproto_connection_on_input(ev_loop *loop, struct ev_io *watcher,
			   int /* revents */)
{
	struct iproto_connection *con =
		(struct iproto_connection *) watcher->data;
	int fd = con->input.fd;
	assert(fd >= 0);

	try {
		/* Ensure we have sufficient space for the next round.  */
		struct iobuf *iobuf = iproto_connection_input_iobuf(con);
		if (iobuf == NULL) {
			ev_io_stop(loop, &con->input);
			return;
		}

		struct ibuf *in = &iobuf->in;
		/* Read input. */
		int nrd = sio_read(fd, in->end, ibuf_unused(in));
		if (nrd < 0) {                  /* Socket is not ready. */
			ev_io_start(loop, &con->input);
			return;
		}
		if (nrd == 0) {                 /* EOF */
			iproto_connection_close(con);
			return;
		}
		/* Update the read position and connection state. */
		in->end += nrd;
		con->parse_size += nrd;
		/* Enqueue all requests which are fully read up. */
		iproto_enqueue_batch(con, in);
		/*
		 * Keep reading input, as long as the socket
		 * supplies data.
		 */
		if (!ev_is_active(&con->input))
			ev_feed_event(loop, &con->input, EV_READ);
	} catch (Exception *e) {
		e->log();
		iproto_connection_close(con);
	}
}

/** Get the iobuf which is currently being flushed. */
static inline struct iobuf *
iproto_connection_output_iobuf(struct iproto_connection *con)
{
	if (con->iobuf[1]->out_pos.size > con->write_pos.size)
		return con->iobuf[1];
	else if (con->iobuf[1]->ref_count > 0)
		return NULL;
	/*
	 * Don't try to write from a newer buffer if an older one
	 * exists: in case of a partial write of a newer buffer,
	 * the client may end up getting a salad of different
	 * pieces of replies from both buffers.
	 */
	if (ibuf_size(&con->iobuf[1]->in) == 0 &&
	    con->iobuf[0]->out_pos.size > con->write_pos.size)
		return con->iobuf[0];
	return NULL;
}

/** writev() to the socket and handle the output. */
static int
iproto_flush(struct iobuf *iobuf, int fd, struct obuf_svp *svp)
{
	/* Begin writing from the saved position. */
	struct iovec iov[IOBUF_IOV_MAX];
	int iovcnt = iobuf->out_pos.pos - svp->pos + 1;
	assert(iovcnt > 0);
	ssize_t nwr;
	if (svp->pos == iobuf->out_pos.pos) {
		iov[0].iov_base = (char *) iobuf->out.iov[iobuf->out_pos.pos].iov_base + svp->iov_len;
		iov[0].iov_len = iobuf->out_pos.iov_len - svp->iov_len;
	} else {
		iov[0].iov_base = (char *) iobuf->out.iov[svp->pos].iov_base + svp->iov_len;
		iov[0].iov_len = iobuf->out.iov[svp->pos].iov_len - svp->iov_len;
		iov[iovcnt - 1].iov_base = iobuf->out.iov[iobuf->out_pos.pos].iov_base;
		iov[iovcnt - 1].iov_len = iobuf->out_pos.iov_len;
		for (int i = 1; i < (iovcnt - 1); ++i) {
			iov[i].iov_base = iobuf->out.iov[svp->pos + i].iov_base;
			iov[i].iov_len = iobuf->out.iov[svp->pos + i].iov_len;
		}
	}
	nwr = sio_writev(fd, iov, iovcnt);
	if (nwr > 0) {
		svp->size += nwr;
		if (iobuf->ref_count == 0 &&
			svp->size == obuf_size(&iobuf->out))
		{
			iobuf_reset(iobuf);
			*svp = obuf_create_svp(&iobuf->out);
			return 0;
		}
		sio_move_iov(iobuf, svp, nwr);
		assert(svp->pos <= iobuf->out.pos);
	}
	return -1;
}

static void
iproto_connection_on_output(ev_loop *loop, struct ev_io *watcher,
			    int /* revents */)
{
	struct iproto_connection *con = (struct iproto_connection *) watcher->data;
	int fd = con->output.fd;
	struct obuf_svp *svp = &con->write_pos;

	try {
		struct iobuf *iobuf;
		while ((iobuf = iproto_connection_output_iobuf(con))) {
			if (iproto_flush(iobuf, fd, svp) < 0) {
				ev_io_start(loop, &con->output);
				return;
			}
			if (! ev_is_active(&con->input))
				ev_feed_event(loop, &con->input, EV_READ);
		}
		if (ev_is_active(&con->output))
			ev_io_stop(loop, &con->output);
	} catch (Exception *e) {
		e->log();
		iproto_connection_close(con);
	}
}

/* }}} */

/* {{{ iproto_process_* functions */

static void
iproto_process(struct iproto_request *ireq)
{
	struct obuf *out = &ireq->iobuf->out;
	struct iproto_connection *con = ireq->connection;

	if (unlikely(! evio_is_active(&con->output)))
		return;

	struct obuf_svp svp = obuf_create_svp(out);
	try {
		switch (ireq->header.type) {
		case IPROTO_SELECT:
		case IPROTO_INSERT:
		case IPROTO_REPLACE:
		case IPROTO_UPDATE:
		case IPROTO_DELETE:
			assert(ireq->request.type == ireq->header.type);
			struct iproto_port port;
			iproto_port_init(&port, out, ireq->header.sync);
			box_process(&ireq->request, (struct port *) &port);
			break;
		case IPROTO_CALL:
			assert(ireq->request.type == ireq->header.type);
			stat_collect(stat_base, ireq->request.type, 1);
			box_lua_call(&ireq->request, out);
			break;
		case IPROTO_EVAL:
			assert(ireq->request.type == ireq->header.type);
			stat_collect(stat_base, ireq->request.type, 1);
			box_lua_eval(&ireq->request, out);
			break;
		case IPROTO_AUTH:
		{
			assert(ireq->request.type == ireq->header.type);
			const char *user = ireq->request.key;
			uint32_t len = mp_decode_strl(&user);
			authenticate(user, len, ireq->request.tuple,
				     ireq->request.tuple_end);
			iproto_reply_ok(out, ireq->header.sync);
			break;
		}
		case IPROTO_PING:
			iproto_reply_ok(out, ireq->header.sync);
			break;
		case IPROTO_JOIN:
			box_process_join(con->input.fd, &ireq->header);
			return;
		case IPROTO_SUBSCRIBE:
			box_process_subscribe(con->input.fd, &ireq->header);
			return;
		default:
			tnt_raise(ClientError, ER_UNKNOWN_REQUEST_TYPE,
				   (uint32_t) ireq->header.type);
		}
	} catch (Exception *e) {
		obuf_rollback_to_svp(out, &svp);
		iproto_reply_error(out, e, ireq->header.sync);
	}
}

static struct iproto_request *
iproto_request_new(struct iproto_connection *con,
		   iproto_request_f process, uint8_t type)
{
	struct iproto_request *ireq =
		(struct iproto_request *) mempool_alloc(&iproto_request_pool);
	ireq->connection = con;
	ireq->iobuf = con->iobuf[0];
	ireq->session = con->session;
	ireq->process = process;
	ireq->type = type;
	ireq->result = false;
	return ireq;
}

const char *
iproto_greeting(const char *salt)
{
	static __thread char greeting[IPROTO_GREETING_SIZE + 1];
	char base64buf[SESSION_SEED_SIZE * 4 / 3 + 5];

	base64_encode(salt, SESSION_SEED_SIZE, base64buf, sizeof(base64buf));
	snprintf(greeting, sizeof(greeting),
		 "Tarantool %-20s %-32s\n%-63s\n",
		 tarantool_version(), custom_proc_title, base64buf);
	return greeting;
}

static void
iproto_init_obuf(struct iobuf *iobuf)
{
	if (iobuf->out.pool)
		return;
	struct region *pool = (struct region *)mempool_alloc(&iproto_obuf_pool);
	region_create(pool, &cord()->slabc);
	region_set_name(pool, "obuf_cache");
	iobuf_obuf_init(iobuf, pool);
}
/**
 * Handshake a connection: invoke the on-connect trigger
 * and possibly authenticate. Try to send the client an error
 * upon a failure.
 */
static void
iproto_process_connect(struct iproto_request *request)
{
	struct iproto_connection *con = request->connection;
	struct iobuf *iobuf = request->iobuf;
	int fd = con->input.fd;
	iproto_init_obuf(con->iobuf[0]);
	iproto_init_obuf(con->iobuf[1]);
	try {              /* connect. */
		con->session = session_create(fd, con->cookie);
		obuf_dup(&iobuf->out, iproto_greeting(con->session->salt),
			   IPROTO_GREETING_SIZE);
		if (! rlist_empty(&session_on_connect))
			session_run_on_connect_triggers(con->session);
		request->result = true;
	} catch (Exception *e) {
		iproto_reply_error(&iobuf->out, e, request->header.type);
		return;
	}
}

static void
iproto_process_disconnect(struct iproto_request *request)
{
	/* Runs the trigger, which may yield. */
	iproto_connection_delete(request->connection);
	iobuf_obuf_delete(request->connection->iobuf[0]);
	iobuf_obuf_delete(request->connection->iobuf[1]);
}

/** }}} */

/**
 * Create a connection context and start input.
 */
static void
iproto_on_accept(struct evio_service * /* service */, int fd,
		 struct sockaddr *addr, socklen_t addrlen)
{
	char name[SERVICE_NAME_MAXLEN];
	snprintf(name, sizeof(name), "%s/%s", "iobuf",
		sio_strfaddr(addr, addrlen));

	struct iproto_connection *con;

	con = iproto_connection_new(name, fd, addr);
	/*
	 * Ignore request allocation failure - the queue size is
	 * fixed so there is a limited number of requests in
	 * use, all stored in just a few blocks of the memory pool.
	 */
	struct iproto_request *ireq =
		iproto_request_new(con, iproto_process_connect,
				   iproto_request_connect);
	ireq->total_len = 0;
	SWITCH_TO_TXN
}

static void on_bind(void *arg __attribute__((unused)))
{
	fiber_call(fiber_new("leave_local_hot_standby",
			     (fiber_func) box_leave_local_standby_mode));
}

/** Initialize a read-write port. */
void
iproto_init(const char *uri)
{
	/* Run a primary server. */
	if (!uri)
		return;

	ev_async_init(&iproto_state.txn_event, iproto_queue_schedule);
	ev_async_init(&iproto_state.iproto_event, iproto_response_schedule);
	mempool_create(&iproto_obuf_pool, &cord()->slabc,
			sizeof(struct region));
	rlist_create(&iproto_state.fiber_cache);
	STAILQ_INIT(&iproto_state.txn_queue);
	STAILQ_INIT(&iproto_state.txn_input);
	STAILQ_INIT(&iproto_state.iproto_queue);
	STAILQ_INIT(&iproto_state.iproto_input);
	iproto_state.txn_loop = loop();
	ev_async_start(iproto_state.txn_loop, &iproto_state.txn_event);

	pthread_mutexattr_t errorcheck;
	(void) tt_pthread_mutexattr_init(&errorcheck);
#ifndef NDEBUG
	(void) tt_pthread_mutexattr_settype(&errorcheck, PTHREAD_MUTEX_ERRORCHECK);
#endif
	/* Initialize queue lock mutex. */
	(void) tt_pthread_mutex_init(&iproto_state.mutex, &errorcheck);
	(void) tt_pthread_mutexattr_destroy(&errorcheck);
	(void) tt_pthread_cond_init(&iproto_state.cond, NULL);

	tt_pthread_mutex_lock(&iproto_state.mutex);
	if (cord_start(&iproto_state.cord, "iproto", iproto_thread, (void *)uri))
		tnt_raise(ClientError, ER_CFG, "cant create iproto thread");
	tt_pthread_cond_wait(&iproto_state.cond, &iproto_state.mutex);
	tt_pthread_mutex_unlock(&iproto_state.mutex);
	on_bind(NULL);
}

static void*
iproto_thread(void *worker_args)
{
	const char *uri = (const char *) worker_args;
	tt_pthread_mutex_lock(&iproto_state.mutex);
	iobuf_init();
	static struct evio_service primary;
	evio_service_init(loop(), &primary, "primary",
			  uri, iproto_on_accept, NULL);
	evio_service_start(&primary);
	iproto_state.iproto_loop = loop();
	mempool_create(&iproto_request_pool, &cord()->slabc,
			sizeof(struct iproto_request));
	mempool_create(&iproto_connection_pool, &cord()->slabc,
			sizeof(struct iproto_connection));
	ev_async_start(iproto_state.iproto_loop, &iproto_state.iproto_event);
	tt_pthread_cond_signal(&iproto_state.cond);
	tt_pthread_mutex_unlock(&iproto_state.mutex);
	try {
		ev_run(iproto_state.iproto_loop, 0);
	} catch (...) {
		say_crit("iproto_thread found unhandled exception");
		throw;
	}
	ev_async_stop(iproto_state.iproto_loop, &iproto_state.iproto_event);
	say_info("iproto stopped");
	return NULL;
}

/* vim: set foldmethod=marker */
