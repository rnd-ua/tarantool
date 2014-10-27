#ifndef TARANTOOL_RAFT_SESSION_H_INCLUDED
#define TARANTOOL_RAFT_SESSION_H_INCLUDED
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

#include <list>
#include <boost/asio.hpp>
#include <arpa/inet.h>

#include "box/raft_common.h"

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

using boost::asio::ip::tcp;

class raft_session {
  typedef std::list<std::pair<int, struct iovec*> > write_queue_t;
  typedef void (raft_session::*handler_t)();
public:
  raft_session(tcp::socket socket);
  raft_session(typename raft_host_map_t::iterator i_host);
  void send(const raft_msg_body& b);

private:
  void init_handlers();

  void reconnect();
  void do_connect();
  void do_connect(tcp::resolver::iterator it);
  void handle_connect();

  void send(uint8_t type, const void* data, std::size_t length);
  void send(uint8_t type);
  void send(uint8_t type, const raft_msg_info& h);
  void do_send() {
    if (active_write_ || write_queue_.empty()) return;
    active_write_ = true;
    do_send(0);
  }
  void do_send(int part);
  void push_write_queue(uint8_t type, const void* data, std::size_t size);
  void pop_write_queue();
  void handle_send(boost::system::error_code ec);

  uint64_t decode(xrow_header* b);
  void decode(raft_msg_info* info);
  void decode_header() {
    read_header_.length = ntohl(read_header_.length);
  }

  void do_read_header();
  void handle_read(boost::system::error_code ec, std::size_t length);
  void handle_hello();
  void handle_leader_promise();
  void handle_leader_accept();
  void handle_leader_submit();
  void handle_leader_reject();
  void handle_body();
  void handle_submit();
  void handle_reject();

  void fault(boost::system::error_code ec);
  void start_timeout(const boost::posix_time::time_duration& t);
  void on_connected();
  void send_leader_promise();

  bool opened_;
  bool reconnect_wait_;
  bool active_write_;
  raft_header read_header_;
  std::vector<char> read_buff_;
  write_queue_t write_queue_;
  std::vector<uint8_t> write_buff_;
  tcp::socket socket_;
  tcp::resolver resolver_;
  boost::asio::deadline_timer timer_;
  boost::posix_time::time_duration reconnect_timeout_;
  raft_host_map_t::iterator i_host_;
  handler_t handlers_[raft_mtype_count];
};


#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* TARANTOOL_RAFT_SESSION_H_INCLUDED */
