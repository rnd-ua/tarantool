#ifndef TARANTOOL_RAFT_COMMON_H_INCLUDED
#define TARANTOOL_RAFT_COMMON_H_INCLUDED
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

#include <map>
#include <vector>
#include <string>
#include <memory>

#include <boost/asio.hpp>
#include <boost/date_time/time_duration.hpp>

#include "xrow.h"

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

class raft_session;

struct raft_host_data {
  raft_host_data()
    : leader(false), local(false), connected(0), id(-1), port(0)
    , out_session(nullptr), in_session(nullptr)
  {}
  raft_host_data(const raft_host_data& data);
  raft_host_data(raft_host_data&& data)
    : leader(data.leader), local(data.local), connected(data.connected)
    , id(data.id), host(std::move(data.host)), port(data.port)
    , out_session(data.out_session.release()), in_session(data.in_session.release())
    , full_name(std::move(data.full_name))
  {
    buffer.bodycnt = 0;
    buffer.body[0].iov_base = NULL;
    buffer.body[0].iov_len = 0;
  }
  raft_host_data& operator=(const raft_host_data& data);

  bool leader;
  bool local;
  uint8_t connected; // 0 - not connected, 1 - partial connected, 2 - full duplex
  uint8_t id;
  std::string host;
  unsigned short port;
  std::unique_ptr<raft_session> out_session;
  std::unique_ptr<raft_session> in_session;
  std::string full_name;
  xrow_header buffer;
};

typedef std::map<std::string, raft_host_data> raft_host_map_t;
typedef std::vector<raft_host_map_t::iterator> raft_host_index_t;

struct raft_header {
  uint8_t type;
  uint32_t length;
} __attribute__((packed));

struct raft_msg_info {
  raft_msg_info() : gsn(-1), lsn(-1), server_id(0) {}
  raft_msg_info(int64_t g, int64_t l, uint32_t s)
    : gsn(g), lsn(l), server_id(s)
  {}

  int64_t gsn;
  int64_t lsn;
  uint32_t server_id;
} __attribute__((packed));

struct raft_msg_body {
  int64_t gsn;
  xrow_header* body;
} __attribute__((packed));

enum raft_message_type {
  raft_mtype_hello = 0,
  raft_mtype_leader_promise = 1,
  raft_mtype_leader_accept = 2,
  raft_mtype_leader_submit = 3,
  raft_mtype_leader_reject = 4,
  raft_mtype_body = 5,
  raft_mtype_submit = 6,
  raft_mtype_reject = 7,
  raft_mtype_count = 8
};

enum raft_machine_state {
  raft_state_initial = 0,
  raft_state_leader_accept = 1,
  raft_state_ready = 2
};

struct raft_local_state {
  raft_local_state()
    : leader_id(-1), num_leader_accept(0), num_connected(1)
    , max_connected_id(-1), local_id(-1), state(raft_state_initial)
    , gsn(-1), lsn(-1), max_gsn(-1)
  {}

  boost::asio::io_service io_service;

  raft_host_map_t host_map;
  raft_host_index_t host_index;
  int8_t leader_id;
  uint8_t num_leader_accept;
  uint8_t num_connected;
  int8_t max_connected_id;
  int8_t local_id;
  uint8_t state;
  int64_t gsn;
  int64_t lsn;
  int64_t max_gsn;

  /* settings */
  boost::posix_time::time_duration read_timeout;
  boost::posix_time::time_duration write_timeout;
  boost::posix_time::time_duration connect_timeout;
  boost::posix_time::time_duration resolve_timeout;
  boost::posix_time::time_duration reconnect_timeout;
};

extern struct raft_local_state raft_state;

inline bool has_consensus() {
  return raft_state.num_connected > raft_state.host_index.size() / 2;
}
inline bool has_leader() {
  return raft_state.leader_id >= 0;
}
inline bool is_leader() {
  return raft_state.leader_id == raft_state.local_id;
}

void raft_write_wal(uint64_t gsn, uint32_t server_id);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* TARANTOOL_RAFT_COMMON_H_INCLUDED */
