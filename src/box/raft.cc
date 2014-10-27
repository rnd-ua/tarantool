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
#include "raft.h"

#include <list>
#include "cfg.h"
#include "fio.h"
#include "coio.h"
#include "scoped_guard.h"
#include "msgpuck/msgpuck.h"
#include "box.h"
#include "request.h"
#include "port.h"
#include "raft_common.h"
#include "raft_session.h"

static struct wal_writer* wal_local_writer = NULL;
static struct wal_writer proxy_wal_writer;
static struct wal_fifo raft_input = STAILQ_HEAD_INITIALIZER(raft_input);
static pthread_once_t raft_writer_once = PTHREAD_ONCE_INIT;
static ev_async local_write_event;

static std::list<xrow_header> raft_local_input;
static std::list<xrow_header> raft_local_commit;
static fiber* raft_local_f;

/** WAL writer thread routine. */
static void* raft_writer_thread(void*);
static void raft_writer_pop();
static void raft_read_cfg();
static void raft_init_state(const struct vclock* vclock);
static void raft_proceed_queue();

/**
* A pthread_atfork() callback for a child process. Today we only
* fork the master process to save a snapshot, and in the child
* the WAL writer thread is not necessary and not present.
*/
static void
raft_writer_child()
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

/**
* Today a WAL writer is started once at start of the
* server.  Nevertheless, use pthread_once() to make
* sure we can start/stop the writer many times.
*/
static void
raft_writer_init_once()
{
  (void) tt_pthread_atfork(NULL, NULL, raft_writer_child);
}

/**
* A commit watcher callback is invoked whenever there
* are requests in wal_writer->commit. This callback is
* associated with an internal WAL writer watcher and is
* invoked in the front-end main event loop.
*
* A rollback watcher callback is invoked only when there is
* a rollback request and commit is empty.
* We roll back the entire input queue.
*
* ev_async, under the hood, is a simple pipe. The WAL
* writer thread writes to that pipe whenever it's done
* handling a pack of requests (look for ev_async_send()
* call in the writer thread loop).
*/
static void
raft_schedule_queue(struct wal_fifo *queue)
{
  /*
   * Can't use STAILQ_FOREACH since fiber_call()
   * destroys the list entry.
   */
  struct wal_write_request *req, *tmp;
  STAILQ_FOREACH_SAFE(req, queue, wal_fifo_entry, tmp)
    fiber_call(req->fiber);
}

static void
raft_schedule(ev_loop * /* loop */, ev_async *watcher, int /* event */)
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
  raft_schedule_queue(&commit);
  /*
   * Perform a cascading abort of all transactions which
   * depend on the transaction which failed to get written
   * to the write ahead log. Abort transactions
   * in reverse order, performing a playback of the
   * in-memory database state.
   */
  STAILQ_REVERSE(&rollback, wal_write_request, wal_fifo_entry);
  raft_schedule_queue(&rollback);
}

static void raft_local_write(ev_loop * /* loop */, ev_async * /* watcher */, int /* event */) {
  fiber_call(raft_local_f);
}

static void raft_local_write_fiber(va_list /* ap */) {
  while(true) {
    raft_local_f = fiber();
    fiber_yield();
    (void) tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
    raft_local_commit.splice(raft_local_commit.begin(), raft_local_input);
    (void) tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
    if (raft_local_commit.empty()) return;
    for(xrow_header& row : raft_local_commit) {
      struct request req;
      request_create(&req, row.type);
      request_decode(&req, (const char*)row.body[0].iov_base, row.body[0].iov_len);
      req.header = &row;
      box_process(&null_port, &req);
      for (int i = 0; i < row.bodycnt; ++i) {
        free(row.body[i].iov_base);
      }
    }
    raft_local_commit.clear();
  }
}

wal_writer* raft_init(wal_writer* initial, struct vclock *vclock) {
  assert (initial != NULL);
  if (cfg_geti("enable_raft") <= 0) {
    say_info("enable_raft=%d\n", cfg_geti("enable_raft"));
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

  ev_async_init(&proxy_wal_writer.write_event, raft_schedule);
  ev_async_init(&local_write_event, raft_local_write);
  proxy_wal_writer.write_event.data = &proxy_wal_writer;
  proxy_wal_writer.txn_loop = loop();

  (void) tt_pthread_once(&raft_writer_once, raft_writer_init_once);

  proxy_wal_writer.batch = fio_batch_alloc(sysconf(_SC_IOV_MAX));

  if (proxy_wal_writer.batch == NULL)
    panic_syserror("fio_batch_alloc");

  raft_local_f = fiber_new("raft local dump", raft_local_write_fiber);
  /* Create and fill writer->cluster hash */
  vclock_create(&proxy_wal_writer.vclock);
  vclock_copy(&proxy_wal_writer.vclock, vclock);

  ev_async_start(proxy_wal_writer.txn_loop, &proxy_wal_writer.write_event);
  ev_async_start(proxy_wal_writer.txn_loop, &local_write_event);

  /* II. Start the thread. */
  raft_read_cfg();
  raft_init_state(vclock);
  if (cord_start(&proxy_wal_writer.cord, "raft", raft_writer_thread, NULL)) {
    wal_writer_destroy(&proxy_wal_writer);
    return 0;
  }
  fiber_call(raft_local_f);
  return &proxy_wal_writer;
}

int raft_write(struct recovery_state *r, struct xrow_header *row) {
  if (wal_local_writer == NULL) return wal_write_lsn(r, row);
  /* try to sync transaction with other hosts, call wal_write and return result */
  struct wal_writer *writer = r->writer;

  struct wal_write_request *req = (struct wal_write_request *)
    region_alloc(&fiber()->gc, sizeof(struct wal_write_request));

  req->fiber = fiber();
  req->res = -1;
  req->row = row;
  row->tm = ev_now(loop());
  row->sync = 0;

  (void) tt_pthread_mutex_lock(&writer->mutex);
  bool input_was_empty = STAILQ_EMPTY(&writer->input);
  STAILQ_INSERT_TAIL(&writer->input, req, wal_fifo_entry);

  if (input_was_empty)
    raft_writer_pop();

  (void) tt_pthread_mutex_unlock(&writer->mutex);

  fiber_yield(); /* Request was inserted. */

  /* req->res is -1 on error */
  if (req->res < 0)
    return -1; /* error */

  return wal_write(wal_local_writer, req); /* success, send to local wal writer */
}

#include <iterator>
#include <boost/lexical_cast.hpp>

struct raft_execute_info {
  wal_write_request* request;
  int64_t gsn;
  uint8_t submit;
};

static struct raft_execute_info raft_cur_execute;

static void set_cur_execute(wal_write_request* request) {
  raft_cur_execute.request = request;
  raft_cur_execute.gsn = -1;
  raft_cur_execute.submit = 0;
}

static int raft_get_timeout(const char* name, int def) {
  int v = cfg_geti(name);
  if (v < 1) v = def;
  return def;
}

static void raft_init_state(const struct vclock* vclock) {
  raft_state.max_gsn = vclock->lsn[RAFT_SERVER_ID] == -1 ? 0 : vclock->lsn[RAFT_SERVER_ID];
}

static void raft_read_cfg() {
  raft_state.read_timeout = boost::posix_time::milliseconds(raft_get_timeout("raft_read_timeout", 3100));
  raft_state.write_timeout = boost::posix_time::milliseconds(raft_get_timeout("raft_write_timeout", 3100));
  raft_state.connect_timeout = boost::posix_time::milliseconds(raft_get_timeout("raft_connect_timeout", 3100));
  raft_state.resolve_timeout = boost::posix_time::milliseconds(raft_get_timeout("raft_resolve_timeout", 3100));
  raft_state.reconnect_timeout = boost::posix_time::milliseconds(raft_get_timeout("raft_reconnect_timeout", 3100));
  const char* hosts = cfg_gets("raft_replica");
  if (hosts == NULL) {
    tnt_raise(ClientError, ER_CFG, "raft replica: expected host:port[;host_port]*");
  }
  std::string hosts_str(hosts);
  auto i_host_begin = hosts_str.begin();
  auto i_host_end = hosts_str.end();
  while (i_host_begin != hosts_str.end()) {
    i_host_end = std::find(i_host_begin, hosts_str.end(), ';');
    auto i_url = std::find(i_host_begin, i_host_end, ':');
    raft_host_data nhost;
    nhost.host.assign(i_host_begin, i_url);
    try {
      nhost.port = boost::lexical_cast<unsigned short>(std::string(i_url + 1, i_host_end));
    } catch (...) {
      tnt_raise(ClientError, ER_CFG, "raft replica: invalid port in raft url");
    }
    nhost.full_name.assign(i_host_begin, i_host_end);
    raft_state.host_map.emplace(nhost.full_name, std::move(nhost));
    i_host_begin = (i_host_end != hosts_str.end() ? i_host_end + 1 : i_host_end);
  }
  const char* localhost = cfg_gets("raft_local");
  if (localhost == NULL) {
    tnt_raise(ClientError, ER_CFG, "raft replica: raft_local param not found");
  }
  auto i_local = raft_state.host_map.find(localhost);
  if (i_local == raft_state.host_map.end()) {
    tnt_raise(ClientError, ER_CFG, "raft replica: raft_local param contains unknown host");
  }
  i_local->second.local = true;
}

class raft_server {
public:
  raft_server(const tcp::endpoint& endpoint)
    : acceptor_(raft_state.io_service, endpoint), socket_(raft_state.io_service)
  {
    do_accept();
  }

private:
  void do_accept() {
    acceptor_.async_accept(socket_,
      [this](boost::system::error_code ec) {
        if (!ec) {
          new raft_session(std::move(socket_));
        }
        do_accept();
      });
  }

  tcp::acceptor acceptor_;
  tcp::socket socket_;
};

static void* raft_writer_thread(void*) {
  uint8_t host_id = 0;
  raft_state.host_index.reserve(raft_state.host_map.size());
  for (auto i_host = raft_state.host_map.begin(); i_host != raft_state.host_map.end(); ++i_host) {
    i_host->second.id = host_id++;
    raft_state.host_index.push_back(i_host);
    if (i_host->second.local) {
      raft_state.local_id = i_host->second.id;
    } else {
      i_host->second.out_session.reset(new raft_session(i_host));
    }
  }
  raft_state.max_connected_id = raft_state.local_id;
  assert(raft_state.local_id >= 0);
  // start to listen port
  raft_server acceptor(tcp::endpoint(
    boost::asio::ip::tcp::v4(),
    raft_state.host_index[raft_state.local_id]->second.port
  ));
  // start to make sessions to other hosts
  while (!proxy_wal_writer.is_shutdown) {
#ifdef NDEBUG
    try {
      raft_state.io_service.run();
    } catch (const std::exception& e) {
      say_error("unhandled std::exception from network, what='%s'", e.what());
    } catch (const Exception& e) {
      say_error("unhandled tarantool exception from network, what='%s'", e.errmsg());
    } catch (const boost::system::error_code& e) {
      say_error("unhandled boost::system::error_code exception from network, code=%d, what='%s'",
        e.value(), e.message().c_str());
    } catch (...) {
      say_error("unhandled unknown exception from network");
    }
#else
    raft_state.io_service.run();
#endif
  }
  return NULL;
}

void raft_write_wal(uint64_t gsn, uint32_t server_id) {
  if (server_id == raft_state.local_id) {
    raft_cur_execute.request->row->server_id = RAFT_SERVER_ID;
    raft_cur_execute.request->row->lsn = gsn;
    raft_cur_execute.request->res = 1;
    STAILQ_INSERT_HEAD(&proxy_wal_writer.commit, raft_cur_execute.request, wal_fifo_entry);
    raft_cur_execute.request = NULL;
    ev_async_send(proxy_wal_writer.txn_loop, &proxy_wal_writer.write_event);
    raft_proceed_queue();
  } else {
    raft_state.host_index[server_id]->second.buffer.server_id = RAFT_SERVER_ID;
    raft_state.host_index[server_id]->second.buffer.lsn = gsn;
    tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
    raft_local_input.emplace_back(std::move(raft_state.host_index[server_id]->second.buffer));
    raft_state.host_index[server_id]->second.buffer.body[0].iov_base = NULL;
    tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
    ev_async_send(proxy_wal_writer.txn_loop, &local_write_event);
  }
}

static void raft_proceed_queue() {
  if (STAILQ_EMPTY(&raft_input)) {
    tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
    STAILQ_CONCAT(&raft_input, &proxy_wal_writer.input);
    tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
  }
  if (STAILQ_EMPTY(&raft_input)) {
    return;
  }
  wal_write_request* wreq = STAILQ_FIRST(&raft_input);
  if (wreq->row->server_id == RAFT_SERVER_ID) {
    STAILQ_INSERT_HEAD(&proxy_wal_writer.commit, wreq, wal_fifo_entry);
    STAILQ_REMOVE_HEAD(&raft_input, wal_fifo_entry);
    wreq->res = 1;
    ev_async_send(proxy_wal_writer.txn_loop, &proxy_wal_writer.write_event);
  } else {
    if (raft_cur_execute.request) return;
    STAILQ_REMOVE_HEAD(&raft_input, wal_fifo_entry);
    set_cur_execute(wreq);
    raft_msg_body msg;
    raft_cur_execute.gsn = msg.gsn = (raft_state.leader_id == raft_state.local_id ? ++raft_state.gsn : 0);
    raft_cur_execute.request->row->lsn = ++raft_state.lsn;
    raft_cur_execute.request->row->server_id = raft_state.local_id + 1;
    msg.body = raft_cur_execute.request->row;
    for (auto &host : raft_state.host_index) {
      if (!host->second.local && host->second.connected == 2)
        host->second.out_session->send(msg);
    }
  }
}

static void raft_writer_pop() {
  raft_state.io_service.post(&raft_proceed_queue);
}

void raft_writer_stop(struct recovery_state *r) {
  if (wal_local_writer != NULL) {
    (void) tt_pthread_mutex_lock(&proxy_wal_writer.mutex);
    proxy_wal_writer.is_shutdown= true;
    (void) tt_pthread_cond_signal(&proxy_wal_writer.cond);
    raft_state.io_service.stop();
    (void) tt_pthread_mutex_unlock(&proxy_wal_writer.mutex);
    if (cord_join(&proxy_wal_writer.cord)) {
      /* We can't recover from this in any reasonable way. */
      panic_syserror("RAFT writer: thread join failed");
    }
    ev_async_stop(proxy_wal_writer.txn_loop, &proxy_wal_writer.write_event);
    ev_async_stop(proxy_wal_writer.txn_loop, &local_write_event);
    wal_writer_destroy(&proxy_wal_writer);
    r->writer = wal_local_writer;
    wal_local_writer = NULL;
  }
  wal_writer_stop(r);
}
