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
#include "box/raft_zcopy_buff.h"

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

using boost::asio::ip::tcp;

class raft_session {
  typedef void (raft_session::*handler_t)();
public:
  raft_session(tcp::socket socket);
  raft_session(uint32_t server_id);
  void send(uint8_t type, const raft_msg_body& b);
  void send(uint8_t type, uint64_t gsn);
  void send(uint8_t type);

  void fault(boost::system::error_code ec);

private:
  void init_handlers();

  void reconnect();
  void do_connect();
  void do_connect(tcp::resolver::iterator it);
  void handle_connect();

  void send(uint8_t type, const raft_msg_info& h);
  void send_hello();
  void do_send();

  uint64_t decode_body();
  void decode(raft_msg_info* info);
  uint32_t decode_state();
  void decode_header() {
    read_header_.length = ntohl(read_header_.length);
  }
  uint64_t decode_gsn();

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
  void handle_proxy_request();
  void handle_proxy_response();

  void start_timeout(const boost::posix_time::time_duration& t);
  void start_recover();
  void on_connected();

  bool opened_;
  bool reconnect_wait_;
  bool active_write_;
  raft_header read_header_;
  std::vector<char> read_buff_;
  raft_zcopy_buff write_queue_;
  raft_zcopy_buff write_buff_;
  tcp::socket socket_;
  tcp::resolver resolver_;
  boost::asio::deadline_timer timer_;
  boost::posix_time::time_duration reconnect_timeout_;
  raft_host_data* host_;
  handler_t handlers_[raft_mtype_count];
};


#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* TARANTOOL_RAFT_SESSION_H_INCLUDED */
