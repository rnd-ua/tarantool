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
  , i_host_(raft_state.host_map.end())
{
  init_handlers();
  do_read_header();
}

raft_session::raft_session(typename raft_host_map_t::iterator i_host)
  : opened_(false), reconnect_wait_(false), active_write_(false)
  , socket_(raft_state.io_service), resolver_(raft_state.io_service)
  , timer_(raft_state.io_service), reconnect_timeout_(raft_state.reconnect_timeout)
  , i_host_(i_host)
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
    endpoint.address(boost::asio::ip::address::from_string(i_host_->second.host));
    endpoint.port(i_host_->second.port);
    start_timeout(raft_state.connect_timeout);
    socket_.async_connect(endpoint, [this](boost::system::error_code ec) {
      CANCEL_TIMER
      if (!ec) handle_connect();
      else fault(ec);
    });
  } catch (...) {
    start_timeout(raft_state.resolve_timeout);
    resolver_.async_resolve(i_host_->second.host,
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
  send(raft_mtype_hello, &raft_state.host_index[raft_state.local_id]->second.full_name[0],
    raft_state.host_index[raft_state.local_id]->second.full_name.size());
  on_connected();
}


void raft_session::send(const raft_msg_body& b) {
  enum { HEADER_LEN_MAX = 64 };
  struct iovec* vec = (struct iovec*)calloc(b.body->bodycnt + 1, sizeof(iovec));
  vec[0].iov_len = sizeof(raft_header) + HEADER_LEN_MAX;
  vec[0].iov_base = calloc(vec[0].iov_len, 1);
  char* begin = ((char*)vec[0].iov_base) + sizeof(raft_header);
  char* pos = begin;
  ((raft_header*)vec[0].iov_base)->type = raft_mtype_body;
  pos = mp_encode_uint(pos, b.gsn);
  pos = mp_encode_uint(pos, b.body->lsn);
  pos = mp_encode_uint(pos, b.body->server_id);
  pos = mp_encode_uint(pos, b.body->sync);
  pos = mp_encode_uint(pos, b.body->type);
//    pos = mp_encode_double(pos, b.body->tm);
  pos = mp_encode_uint(pos, b.body->bodycnt);
  for (int i = 0; i < b.body->bodycnt; ++i) {
    pos = mp_encode_uint(pos, b.body->body[i].iov_len);
  }
  vec[0].iov_len = pos - (char*)vec[0].iov_base;
  ((raft_header*)vec[0].iov_base)->length = (uint32_t)(pos - begin);
  for (int i = 0; i < b.body->bodycnt; ++i) {
    vec[i+1].iov_len = b.body->body[i].iov_len;
    vec[i+1].iov_base = calloc(vec[i+1].iov_len, 1);
    memcpy(vec[i+1].iov_base, b.body->body[i].iov_base, vec[i+1].iov_len);
    ((raft_header*)vec[0].iov_base)->length += vec[i+1].iov_len;
  }
  say_info("[%tX] send message to %s, type %d, size %d",
      (ptrdiff_t)this, i_host_->second.full_name.c_str(), ((raft_header*)vec[0].iov_base)->type,
      (int)((raft_header*)vec[0].iov_base)->length);
  ((raft_header*)vec[0].iov_base)->length = htonl(((raft_header*)vec[0].iov_base)->length);
  write_queue_.emplace_front(b.body->bodycnt + 1, vec);
  do_send();
}

void raft_session::send(uint8_t type, const void* data, std::size_t length) {
  assert(this == i_host_->second.out_session.get());
  say_debug("[%tX] send message to %s, type %d, size %d", (ptrdiff_t)this, i_host_->second.full_name.c_str(), (int)type, (int)length);
  push_write_queue(type, data, length);
  do_send();
}

void raft_session::send(uint8_t type) {
  assert(this == i_host_->second.out_session.get());
  say_debug("[%tX] send message to %s, type %d, size 0", (ptrdiff_t)this, i_host_->second.full_name.c_str(), (int)type);
  push_write_queue(type, 0, 0);
  do_send();
}

void raft_session::send(uint8_t type, const raft_msg_info& h) {
  struct iovec* vec = (struct iovec*)calloc(1, sizeof(iovec));
  vec[0].iov_len = sizeof(raft_header) + sizeof(raft_msg_info);
  vec[0].iov_base = calloc(vec[0].iov_len, 1);
  char* begin = ((char*)vec[0].iov_base) + sizeof(raft_header);
  char* pos = begin;
  ((raft_header*)vec[0].iov_base)->type = type;
  pos = mp_encode_uint(pos, h.gsn);
  pos = mp_encode_uint(pos, h.lsn);
  pos = mp_encode_uint(pos, h.server_id);
  say_debug("[%tX] send message to %s, type %d, size %d",
      (ptrdiff_t)this, i_host_->second.full_name.c_str(), (int)type, (int)(pos - begin));
  ((raft_header*)vec[0].iov_base)->length = htonl((uint32_t)(pos - begin));
  vec[0].iov_len = pos - (char*)vec[0].iov_base;
  write_queue_.emplace_front(1, vec);
  do_send();
}

void raft_session::do_send(int part) {
  start_timeout(raft_state.write_timeout);
  boost::asio::async_write(socket_,
    boost::asio::buffer(write_queue_.back().second[part].iov_base, write_queue_.back().second[part].iov_len),
    boost::asio::transfer_all(),
    [this, part] (boost::system::error_code ec, std::size_t /*length*/) {
      CANCEL_TIMER
      if (!ec) {
        if (write_queue_.back().first > (part + 1)) do_send(part + 1);
        else {
          handle_send(ec);
        }
      } else {
        fault(ec);
      }
    });
}

void raft_session::push_write_queue(uint8_t type, const void* data, std::size_t size) {
  assert(size < UINT32_MAX);
  struct iovec* vec = NULL;
  if (data) vec = (struct iovec*)calloc(2, sizeof(iovec));
  else vec = (struct iovec*)calloc(1, sizeof(iovec));
  vec[0].iov_len = sizeof(raft_header);
  vec[0].iov_base = calloc(1, sizeof(raft_header));
  ((raft_header*)vec[0].iov_base)->type = type;
  ((raft_header*)vec[0].iov_base)->length = htonl((uint32_t)size);
  if (data) {
    vec[1].iov_len = size;
    vec[1].iov_base = calloc(size, 1);
    memcpy(vec[1].iov_base, data, size);
    write_queue_.emplace_front(2, vec);
  } else {
    write_queue_.emplace_front(1, vec);
  }
}

void raft_session::pop_write_queue() {
  for (int i = 0; i < write_queue_.back().first; ++i) {
    free(write_queue_.back().second[i].iov_base);
  }
  free(write_queue_.back().second);
  write_queue_.pop_back();
}

void raft_session::handle_send(boost::system::error_code ec) {
  active_write_ = false;
  pop_write_queue();
  if (!ec) {
    socket_.get_io_service().post(boost::bind(&raft_session::do_send, this));
  } else {
    fault(ec);
  }
}

uint64_t raft_session::decode(xrow_header* b) {
  const char* pos = &read_buff_[0];
  uint64_t gsn = mp_decode_uint(&pos);
  b->lsn = mp_decode_uint(&pos);
  b->server_id = mp_decode_uint(&pos);
  b->sync = mp_decode_uint(&pos);
  b->type = mp_decode_uint(&pos);
//    b->tm = mp_decode_double(&pos);
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
    if (i_host_ != raft_state.host_map.end()) {
      say_debug("[%tX] receive message from %s, code %d, size %d",
          (ptrdiff_t)this, i_host_->second.full_name.c_str(), (int) read_header_.type, (int)length);
    }
    assert(read_header_.type < raft_mtype_count);
    (this->*handlers_[read_header_.type])();
    do_read_header();
  } else {
    fault(ec);
  }
}

void raft_session::handle_hello() {
  std::string remote_host(read_buff_.begin(), read_buff_.end());
  i_host_ = raft_state.host_map.find(remote_host);
  assert(i_host_ != raft_state.host_map.end());
  i_host_->second.in_session.reset(this);
  on_connected();
  if (i_host_->second.out_session) i_host_->second.out_session->reconnect();
}

void raft_session::handle_leader_promise() {
  if (i_host_->second.id == raft_state.max_connected_id && raft_state.state == raft_state_initial) {
    raft_state.state = raft_state_leader_accept;
    say_info("[%tX] raft state changed to leader_accept, possible leader are '%s'",
        (ptrdiff_t)this, raft_state.host_index[raft_state.max_connected_id]->second.full_name.c_str());
    i_host_->second.out_session->send(raft_mtype_leader_accept);
  } else {
    i_host_->second.out_session->send(raft_mtype_leader_reject);
  }
}

void raft_session::handle_leader_accept() {
  if (++raft_state.num_leader_accept > raft_state.host_index.size() / 2) {
    raft_state.leader_id = raft_state.local_id;
    raft_state.state = raft_state_ready;

    raft_state.gsn = raft_state.max_gsn;
    say_info("[%tX] raft state changed to ready, new leader are '%s', max_gsn=%d",
        (ptrdiff_t)this, raft_state.host_index[raft_state.leader_id]->second.full_name.c_str(), (int)raft_state.max_gsn);
    for (auto& host : raft_state.host_index)
      if (!host->second.local && host->second.connected == 2)
        host->second.out_session->send(raft_mtype_leader_submit);
  }
}

void raft_session::handle_leader_submit() {
  raft_state.state = raft_state_ready;
  raft_state.leader_id = i_host_->second.id;
  say_info("[%tX] raft state changed to ready, new leader are '%s'",
      (ptrdiff_t)this, raft_state.host_index[raft_state.leader_id]->second.full_name.c_str());
}

void raft_session::handle_leader_reject() {
  if (raft_state.state == raft_state_leader_accept) {
    raft_state.state = raft_state_initial;
    send_leader_promise();
  }
}

void raft_session::handle_body() {
  uint64_t gsn = decode(&i_host_->second.buffer);
  if (gsn) {
    raft_msg_info info(gsn, i_host_->second.buffer.lsn, i_host_->second.buffer.server_id);
    raft_write_wal(gsn, i_host_->second.id);
    i_host_->second.out_session->send(raft_mtype_submit, info);
  } else if (raft_state.local_id == raft_state.leader_id) {
    // check conflicts and send submit or reject
    gsn = ++raft_state.gsn;
    raft_msg_info info(gsn, i_host_->second.buffer.lsn, i_host_->second.buffer.server_id);
    raft_write_wal(gsn, i_host_->second.id);
    for (auto& host : raft_state.host_index)
      if (!host->second.local && host->second.connected == 2)
        host->second.out_session->send(raft_mtype_submit, info);
  }
}

void raft_session::handle_submit() {
  raft_msg_info info;
  decode(&info);
  raft_write_wal(info.gsn, info.server_id - 1);
  if ((info.server_id - 1) != raft_state.local_id) {
    const xrow_header* h = &raft_state.host_index[info.server_id - 1]->second.buffer;
    for (auto i = 0; i < h->bodycnt; ++i) {
      free(h->body[i].iov_base);
    }
  }
}

void raft_session::handle_reject() {
  raft_msg_info info;
  decode(&info);
  if ((info.server_id - 1) == raft_state.local_id) {
    // rollback operations
  } else {
    const xrow_header* h = &raft_state.host_index[info.server_id - 1]->second.buffer;
    for (auto i = 0; i < h->bodycnt; ++i) {
      free(h->body[i].iov_base);
    }
  }
}

void raft_session::fault(boost::system::error_code ec) {
  if (i_host_ == raft_state.host_map.end()) {
    say_error("[%tX] fault network with unknown, reason: '%s', code: %d", (ptrdiff_t)this, ec.message().c_str(), ec.value());
  } else {
    say_error("[%tX] fault network with %s:%d, reason: '%s', code: %d", (ptrdiff_t)this, i_host_->second.host.c_str(),
        i_host_->second.port, ec.message().c_str(), ec.value());
  }
  if (opened_) {
    if (ec != boost::asio::error::eof) {
      socket_.shutdown(boost::asio::socket_base::shutdown_both);
    }
    socket_.close();
  }
  if (i_host_ == raft_state.host_map.end()) {
    delete this;
    return;
  }
  if (i_host_->second.out_session.get() != this) {
    i_host_->second.out_session->fault(ec);
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
  if (--i_host_->second.connected == 0) return;
  --raft_state.num_connected;
  if (i_host_->second.id == raft_state.leader_id) {
    raft_state.leader_id = -1;
    raft_state.state = raft_state_initial;
    if (i_host_->second.id == raft_state.max_connected_id) {
      for (--raft_state.max_connected_id; raft_state.max_connected_id >= 0; --raft_state.max_connected_id) {
        if (raft_state.host_index[raft_state.max_connected_id]->second.connected == 2) break;
      }
    }
    send_leader_promise();
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
      (ptrdiff_t)this, i_host_->second.full_name.c_str(), (int)(i_host_->second.connected + 1));
  if (++i_host_->second.connected != 2) return;
  ++raft_state.num_connected;
  if (i_host_->second.id > raft_state.max_connected_id)
    raft_state.max_connected_id = i_host_->second.id;
  if (raft_state.leader_id == raft_state.local_id) {
    i_host_->second.out_session->send(raft_mtype_leader_submit);
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
        raft_state.host_index[raft_state.max_connected_id]->second.full_name.c_str());
    raft_state.num_leader_accept = 1;
    for (auto& host : raft_state.host_index) {
      if (!host->second.local && host->second.connected == 2)
        host->second.out_session->send(raft_mtype_leader_promise);
    }
  }
}
