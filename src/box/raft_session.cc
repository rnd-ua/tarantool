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

#include "box/raft_session.h"

#include <boost/bind.hpp>

#include "say.h"
#include "msgpuck/msgpuck.h"

#define CANCEL_TIMER if (ec != boost::asio::error::operation_aborted) timer_.cancel();

raft_session::raft_session(tcp::socket socket)
  : opened_(true), reconnect_wait_(false), active_write_(false)
  , socket_(std::move(socket)), resolver_(socket.get_io_service())
  , timer_(socket.get_io_service()), reconnect_timeout_(boost::posix_time::not_a_date_time)
  , host_(NULL)
{
  init_handlers();
  do_read_header();
}

raft_session::raft_session(uint32_t server_id)
  : opened_(false), reconnect_wait_(false), active_write_(false)
  , socket_(raft_state.io_service), resolver_(raft_state.io_service)
  , timer_(raft_state.io_service), reconnect_timeout_(raft_state.reconnect_timeout)
  , host_(&raft_state.host_index[server_id])
{
  init_handlers();
  do_connect();
}

void raft_session::init_handlers() {
  handlers_[raft_mtype_hello] = &raft_session::handle_hello;
  handlers_[raft_mtype_leader_promise] = &raft_session::handle_leader_promise;
  handlers_[raft_mtype_leader_accept] = &raft_session::handle_leader_accept;
  handlers_[raft_mtype_leader_submit] = &raft_session::handle_leader_submit;
  handlers_[raft_mtype_leader_reject] = &raft_session::handle_leader_reject;
  handlers_[raft_mtype_body] = &raft_session::handle_body;
  handlers_[raft_mtype_submit] = &raft_session::handle_submit;
  handlers_[raft_mtype_reject] = &raft_session::handle_reject;
  handlers_[raft_mtype_proxy_request] = &raft_session::handle_proxy_request;
  handlers_[raft_mtype_proxy_response] = &raft_session::handle_proxy_response;
}

void raft_session::reconnect() {
  if (reconnect_wait_) {
    timer_.cancel();
    do_connect();
  }
}

void raft_session::do_connect() {
  try {
    tcp::endpoint endpoint;
    endpoint.address(boost::asio::ip::address::from_string(host_->host));
    endpoint.port(host_->port);
    start_timeout(raft_state.connect_timeout);
    socket_.async_connect(endpoint, [this](boost::system::error_code ec) {
      CANCEL_TIMER
      if (!ec) handle_connect();
      else fault(ec);
    });
  } catch (...) {
    start_timeout(raft_state.resolve_timeout);
    resolver_.async_resolve(host_->host,
      [this] (boost::system::error_code ec, tcp::resolver::iterator it) {
        if (ec != boost::asio::error::operation_aborted) timer_.cancel();
        if (!ec) do_connect(it); else fault(ec);
      });
  }
}

void raft_session::do_connect(tcp::resolver::iterator it) {
  start_timeout(raft_state.connect_timeout);
  boost::asio::async_connect(socket_, it,
    [this] (boost::system::error_code ec, tcp::resolver::iterator it) {
      CANCEL_TIMER
      if (!ec) handle_connect();
      else if (it++ != tcp::resolver::iterator()) do_connect(it);
      else fault(ec);
    });
}

void raft_session::handle_connect() {
  opened_ = true;
  send_hello();
  on_connected();
}

void raft_session::send(uint8_t type, const raft_msg_body& b) {
  enum { HEADER_LEN_MAX = 64 };
  char* buff = (char*)calloc(sizeof(raft_header) + HEADER_LEN_MAX, 1);
  char* begin = buff + sizeof(raft_header);
  char* pos = begin;
  ((raft_header*)buff)->type = type;
  pos = mp_encode_uint(pos, b.gsn);
  pos = mp_encode_uint(pos, b.body->lsn);
  pos = mp_encode_uint(pos, b.body->server_id);
  pos = mp_encode_uint(pos, b.body->sync);
  pos = mp_encode_uint(pos, b.body->type);
  pos = mp_encode_double(pos, b.body->tm);
  pos = mp_encode_uint(pos, b.body->bodycnt);
  for (int i = 0; i < b.body->bodycnt; ++i) {
    pos = mp_encode_uint(pos, b.body->body[i].iov_len);
  }
  ((raft_header*)buff)->length = (uint32_t)(pos - begin);
  write_queue_.push(boost::asio::const_buffer(buff, pos - buff));
  for (int i = 0; i < b.body->bodycnt; ++i) {
    void* body_buff = calloc(b.body->body[i].iov_len, 1);
    memcpy(body_buff, b.body->body[i].iov_base, b.body->body[i].iov_len);
    write_queue_.push(std::move(boost::asio::const_buffer(body_buff, b.body->body[i].iov_len)));
    ((raft_header*)buff)->length += b.body->body[i].iov_len;
  }
  say_info("[%tX] send message to %s, type %d, size %d",
      (ptrdiff_t)this, host_->full_name.c_str(), type,
      (int)((raft_header*)buff)->length);
  ((raft_header*)buff)->length = htonl(((raft_header*)buff)->length);
  do_send();
}

void raft_session::send(uint8_t type) {
  assert(this == host_->out_session.get());
  say_debug("[%tX] send message to %s, type %d, size 0", (ptrdiff_t)this, host_->full_name.c_str(), (int)type);
  char* buff = (char*)calloc(1, sizeof(raft_header));
  ((raft_header*)buff)->type = type;
  ((raft_header*)buff)->length = 0;
  write_queue_.push(boost::asio::const_buffer(buff, sizeof(raft_header)));
  do_send();
}

void raft_session::send(uint8_t type, uint64_t gsn) {
  char* buff = (char*)calloc(sizeof(raft_header) + sizeof(uint64_t), 1);
  char* begin = buff + sizeof(raft_header);
  char* pos = mp_encode_uint(begin, gsn);
  say_debug("[%tX] send message to %s, type %d, size %d",
      (ptrdiff_t)this, host_->full_name.c_str(), (int)type, (int)(pos - begin));
  ((raft_header*)buff)->type = type;
  ((raft_header*)buff)->length = htonl((uint32_t)(pos - begin));
  write_queue_.push(boost::asio::const_buffer(buff, pos - buff));
  do_send();
}

void raft_session::send(uint8_t type, const raft_msg_info& h) {
  char* buff = (char*)calloc(sizeof(raft_header) + sizeof(raft_msg_info), 1);
  char* begin = buff + sizeof(raft_header);
  char* pos = begin;
  pos = mp_encode_uint(pos, h.gsn);
  pos = mp_encode_uint(pos, h.lsn);
  pos = mp_encode_uint(pos, h.server_id);
  say_debug("[%tX] send message to %s, type %d, size %d",
      (ptrdiff_t)this, host_->full_name.c_str(), (int)type, (int)(pos - begin));
  ((raft_header*)buff)->type = type;
  ((raft_header*)buff)->length = htonl((uint32_t)(pos - begin));
  write_queue_.push(boost::asio::const_buffer(buff, pos - buff));
  do_send();
}

void raft_session::send_hello() {
  char* buff = (char*)calloc(sizeof(raft_header) + sizeof(raft_host_state), 1);
  char* begin = buff + sizeof(raft_header);
  char* pos = begin;
  pos = mp_encode_uint(pos, raft_state.local_id);
  pos = mp_encode_uint(pos, raft_state.max_gsn);
  say_debug("[%tX] send message to %s, type %d, size %d",
      (ptrdiff_t)this, host_->full_name.c_str(), raft_mtype_hello, (int)(pos - begin));
  ((raft_header*)buff)->type = raft_mtype_hello;
  ((raft_header*)buff)->length = htonl((uint32_t)(pos - begin));
  write_queue_.push(boost::asio::const_buffer(buff, pos - buff));
  do_send();
}

void raft_session::do_send() {
  if (active_write_ || write_queue_.size() == 0) return;
  active_write_ = true;
  write_buff_ = write_queue_.split();
  start_timeout(raft_state.write_timeout);
  boost::asio::async_write(socket_, write_buff_, boost::asio::transfer_at_least(write_buff_.size()),
    [this](boost::system::error_code ec, std::size_t /*length*/) {
      CANCEL_TIMER
      write_buff_.clear();
      active_write_ = false;
      if (!ec) socket_.get_io_service().post(boost::bind(&raft_session::do_send, this));
      else fault(ec);
    });
}

uint64_t raft_session::decode(xrow_header* b) {
  const char* pos = &read_buff_[0];
  uint64_t gsn = mp_decode_uint(&pos);
  b->lsn = mp_decode_uint(&pos);
  b->server_id = mp_decode_uint(&pos);
  b->sync = mp_decode_uint(&pos);
  b->type = mp_decode_uint(&pos);
  b->tm = mp_decode_double(&pos);
  b->bodycnt = mp_decode_uint(&pos);
  b->body[0].iov_len = 0;
  for (auto i = 0; i < b->bodycnt; ++i) b->body[0].iov_len += mp_decode_uint(&pos);
  if (b->body[0].iov_base) {
    free(b->body[0].iov_base);
  }
  b->body[0].iov_base = calloc(b->body[0].iov_len, 1);
  memcpy(b->body[0].iov_base, pos, b->body[0].iov_len);
  b->bodycnt = 1;
  return gsn;
}

void raft_session::decode(raft_msg_info* info) {
  const char* pos = &read_buff_[0];
  info->gsn = mp_decode_uint(&pos);
  info->lsn = mp_decode_uint(&pos);
  info->server_id = mp_decode_uint(&pos);
}

void raft_session::decode(raft_host_state* state) {
  const char* pos = &read_buff_[0];
  state->server_id = mp_decode_uint(&pos);
  state->gsn = mp_decode_uint(&pos);
}

uint64_t raft_session::decode_gsn() {
  const char* pos = &read_buff_[0];
  return mp_decode_uint(&pos);
}

void raft_session::do_read_header() {
  boost::asio::async_read(socket_, boost::asio::buffer(&read_header_, sizeof(raft_header)),
    boost::asio::transfer_at_least(sizeof(raft_header)),
    [this](boost::system::error_code ec, std::size_t /* length */) {
      CANCEL_TIMER
      if (!ec) {
        decode_header();
        if (read_header_.length) {
          read_buff_.resize(read_header_.length);
          start_timeout(raft_state.read_timeout);
          boost::asio::async_read(socket_, boost::asio::buffer(&read_buff_[0], read_buff_.size()),
            boost::asio::transfer_at_least(read_buff_.size()),
            [this](boost::system::error_code ec, std::size_t length) {
            socket_.get_io_service().post(boost::bind(&raft_session::handle_read, this, ec, length));
          });
        } else {
          read_buff_.clear();
          handle_read(ec, 0);
        }
      } else {
        fault(ec);
      }
  });
}

void raft_session::handle_read(boost::system::error_code ec, std::size_t length) {
  (void)length;
  CANCEL_TIMER
  if (!ec) {
    assert(length == read_buff_.size());
    if (host_) {
      say_debug("[%tX] receive message from %s, code %d, size %d",
          (ptrdiff_t)this, host_->full_name.c_str(), (int) read_header_.type, (int)length);
    }
    assert(read_header_.type < raft_mtype_count);
    (this->*handlers_[read_header_.type])();
    do_read_header();
  } else {
    fault(ec);
  }
}

void raft_session::handle_hello() {
  raft_host_state state;
  decode(&state);
  assert(state.server_id < raft_state.host_index.size());
  host_ = &raft_state.host_index[state.server_id];
  raft_state.host_state.insert(state);
  host_->in_session.reset(this);
  on_connected();
  if (host_->out_session) host_->out_session->reconnect();
}

void raft_session::handle_leader_promise() {
  if (host_->id == raft_state.max_connected_id && raft_state.state == raft_state_initial) {
    raft_state.state = raft_state_leader_accept;
    say_info("[%tX] raft state changed to leader_accept, possible leader are '%s'",
        (ptrdiff_t)this, raft_state.host_index[raft_state.max_connected_id].full_name.c_str());
    host_->out_session->send(raft_mtype_leader_accept);
  } else {
    host_->out_session->send(raft_mtype_leader_reject);
  }
}

void raft_session::handle_leader_accept() {
  if (++raft_state.num_leader_accept > raft_state.host_index.size() / 2) {
    raft_state.leader_id = raft_state.local_id;
    raft_state.state = raft_state_ready;

    raft_state.gsn = raft_state.max_gsn;
    say_info("[%tX] raft state changed to ready, new leader are '%s', max_gsn=%d",
        (ptrdiff_t)this, raft_state.host_index[raft_state.leader_id].full_name.c_str(), (int)raft_state.max_gsn);
    for (auto& host : raft_state.host_index)
      if (!host.local && host.connected == 2)
        host.out_session->send(raft_mtype_leader_submit);
  }
}

void raft_session::handle_leader_submit() {
  raft_state.state = raft_state_ready;
  raft_state.leader_id = host_->id;
  say_info("[%tX] raft state changed to ready, new leader are '%s'",
      (ptrdiff_t)this, raft_state.host_index[raft_state.leader_id].full_name.c_str());
}

void raft_session::handle_leader_reject() {
  if (raft_state.state == raft_state_leader_accept) {
    raft_state.state = raft_state_initial;
    send_leader_promise();
  }
}

void raft_session::handle_body() {
  uint64_t gsn = decode(&host_->buffer);
  if (host_->id == raft_state.leader_id) {
    raft_state.gsn = gsn;
    raft_write_wal_remote(gsn, host_->id);
  } else {
    say_error("[%tX] receive insert operation from slave %s, gsn %td, lsn %td", (ptrdiff_t)this, host_->full_name.c_str(), gsn, host_->buffer.lsn);
  }
}

void raft_session::handle_submit() {
  uint64_t gsn = decode_gsn();
  if (raft_state.leader_id != raft_state.local_id) {
    say_error("[%tX] slave receive submit with gsn %td from %s", (ptrdiff_t)this, gsn, host_->full_name.c_str());
    return;
  }
  auto& index = raft_state.operation_index.get<raft_local_state::gsn_hash>();
  auto i_op = index.find(gsn);
  if (i_op != index.end()) {
    if (++i_op->submitted > raft_state.host_index.size() / 2) {
      raft_write_wal_local(*i_op.operator->());
      index.erase(i_op);
    } else {
      say_debug("[%tX] leader received submit with gsn %td from %s status is %d", (ptrdiff_t)this, gsn, host_->full_name.c_str(), (int)i_op->submitted);
    }
  } else {
    say_warn("[%tX] leader received unknown submit with gsn %td from %s", (ptrdiff_t)this, gsn, host_->full_name.c_str());
  }
}

void raft_session::handle_reject() {
  uint64_t gsn = decode_gsn();
  if (raft_state.leader_id != raft_state.local_id) {
    // TODO : reject all operations [gsn, ...)
  } else {
    auto& index = raft_state.operation_index.get<raft_local_state::gsn_hash>();
    auto i_op = index.find(gsn);
    if (i_op != index.end()) {
      if (++i_op->rejected > raft_state.host_index.size() / 2) {
        for (raft_host_data& host : raft_state.host_index) {
          if (!host.local) host.out_session->send(raft_mtype_reject, gsn);
        }
        raft_rollback_local(gsn);
      }
    } else {
      say_warn("[%tX] leader received unknown reject with gsn %td from %s", (ptrdiff_t)this, gsn, host_->full_name.c_str());
    }
  }
}

void raft_session::handle_proxy_request() {
  decode(&host_->buffer);
  if (raft_state.leader_id != raft_state.local_id) {
    say_warn("[%tX] slave received proxy request with lsn %td from %s", (ptrdiff_t)this, host_->buffer.lsn, host_->full_name.c_str());
  } else {
    raft_write_wal_remote(++raft_state.gsn, host_->id);
  }
}

void raft_session::handle_proxy_response() {

}

void raft_session::fault(boost::system::error_code ec) {
  if (host_) {
    say_error("[%tX] fault network with %s:%d, reason: '%s', code: %d", (ptrdiff_t)this, host_->host.c_str(),
        host_->port, ec.message().c_str(), ec.value());
  } else {
    say_error("[%tX] fault network with unknown, reason: '%s', code: %d", (ptrdiff_t)this, ec.message().c_str(), ec.value());
  }
  if (opened_) {
    if (ec != boost::asio::error::eof) {
      socket_.shutdown(boost::asio::socket_base::shutdown_both);
    }
    socket_.close();
  }
  if (!host_) {
    delete this;
    return;
  }
  if (host_->out_session.get() != this) {
    host_->out_session->fault(ec);
  }
  if (!reconnect_timeout_.is_special()) {
    reconnect_wait_ = true;
    timer_.cancel();
    timer_.expires_from_now(reconnect_timeout_);
    timer_.async_wait([this](boost::system::error_code ec) {
      reconnect_wait_ = false;
      if (ec != boost::asio::error::operation_aborted) do_connect();
    });
  }
  if (!opened_) return;
  opened_ = false;
  if (--host_->connected == 0) return;
  --raft_state.num_connected;
  raft_state.host_state.erase(raft_host_state({host_->id, 0}));
  if (host_->id == raft_state.leader_id) {
    raft_state.host_state.erase(raft_host_state({(uint32_t)raft_state.local_id, 0}));
    raft_state.host_state.insert(raft_host_state({(uint32_t)raft_state.local_id, (uint64_t)raft_state.gsn}));
    raft_state.leader_id = -1;
    raft_state.state = raft_state_initial;
    raft_state.max_connected_id = raft_state.host_state.begin()->server_id;
    send_leader_promise();
  } else if (raft_state.leader_id == raft_state.local_id) {
    raft_state.leader_id = -1;
    if (raft_state.operation_index.empty()) return;
    uint64_t gsn = raft_state.operation_index.get<raft_local_state::gsn_hash>().begin()->gsn;
    say_info("[%tX] raft state changed to initial, reject all operations from '%ld'", (ptrdiff_t)this, gsn);
    raft_rollback_local(gsn);
  }
}

void raft_session::start_timeout(const boost::posix_time::time_duration& t) {
  timer_.cancel();
  timer_.expires_from_now(t);
  timer_.async_wait([this](boost::system::error_code ec) {
    if (ec != boost::asio::error::operation_aborted) {
      socket_.cancel();
    }
  });
}

void raft_session::on_connected() {
  say_debug("[%tX] on_connected, host: %s, status: %d",
      (ptrdiff_t)this, host_->full_name.c_str(), (int)(host_->connected + 1));
  if (++host_->connected != 2) return;
  ++raft_state.num_connected;
  raft_state.max_connected_id = raft_state.host_state.begin()->server_id;
  if (raft_state.leader_id == raft_state.local_id) {
    host_->out_session->send(raft_mtype_leader_submit);
  } else {
    send_leader_promise();
  }
}

void raft_session::send_leader_promise() {
  say_info("[%tX] leader status: num_connected=%d, state=%d, local_id=%d, max_id=%d",
      (ptrdiff_t)this, (int)raft_state.num_connected, (int)raft_state.state,
      (int)raft_state.local_id, (int)raft_state.max_connected_id
  );
  if (has_consensus() && raft_state.state == raft_state_initial &&
      raft_state.local_id == raft_state.max_connected_id)
  {
    raft_state.state = raft_state_leader_accept;
    say_info("raft state changed to leader_accept, possible leader are '%s'",
        raft_state.host_index[raft_state.max_connected_id].full_name.c_str());
    raft_state.num_leader_accept = 1;
    for (auto& host : raft_state.host_index) {
      if (!host.local && host.connected == 2)
        host.out_session->send(raft_mtype_leader_promise);
    }
  }
}
