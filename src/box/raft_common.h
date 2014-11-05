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
#include <set>
#include <bitset>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <boost/asio.hpp>
#include <boost/range.hpp>
#include <boost/date_time/time_duration.hpp>
#if !defined(NDEBUG)
#define BOOST_MULTI_INDEX_ENABLE_INVARIANT_CHECKING
#define BOOST_MULTI_INDEX_ENABLE_SAFE_MODE
#endif
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/ordered_index.hpp>

#include "xrow.h"
#include "vclock.h"

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

class raft_session;

#define RAFT_LOCAL_DATA (raft_host_index[raft_state.local_id])

struct raft_host_data {
private:
  raft_host_data(const raft_host_data& data);
  raft_host_data& operator=(const raft_host_data& data);

public:
  raft_host_data()
    : leader(false), local(false), connected(0), id(-1), gsn(0)
    , last_op_crc(0), port(0), out_session(nullptr), in_session(nullptr)
  {}
  raft_host_data(raft_host_data&& data)
    : leader(data.leader), local(data.local), connected(data.connected)
    , id(data.id), host(std::move(data.host)), gsn(data.gsn), last_op_crc(data.last_op_crc)
    , port(data.port), out_session(data.out_session.release()), in_session(data.in_session.release())
    , full_name(std::move(data.full_name))
  {
    buffer.bodycnt = 0;
    buffer.body[0].iov_base = NULL;
    buffer.body[0].iov_len = 0;
  }

  bool leader;
  bool local;
  uint8_t connected; // 0 - not connected, 1 - partial connected, 2 - full duplex
  uint8_t id;
  std::string host;
  uint64_t gsn;
  uint32_t last_op_crc;
  unsigned short port;
  std::unique_ptr<raft_session> out_session;
  std::unique_ptr<raft_session> in_session;
  std::string full_name;
  xrow_header buffer;
  std::list<uint64_t> active_ops;
};

typedef std::vector<raft_host_data> raft_host_index_t;
extern raft_host_index_t raft_host_index;

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
  uint64_t gsn;
  xrow_header* body;
};

struct raft_host_state {
  uint32_t server_id;

  bool operator==(const raft_host_state& s) const {
    return s.server_id == server_id;
  }
};
struct raft_host_state_compare;

inline bool operator==(const raft_host_state& s1, const raft_host_state& s2) {
  return s1.operator==(s2);
}

enum raft_message_type {
  raft_mtype_hello = 0,
  raft_mtype_leader_promise = 1,
  raft_mtype_leader_accept = 2,
  raft_mtype_leader_submit = 3,
  raft_mtype_leader_reject = 4,
  raft_mtype_body = 5,
  raft_mtype_submit = 6,
  raft_mtype_reject = 7,
  raft_mtype_proxy_request = 8,
  raft_mtype_proxy_submit = 9,
  raft_mtype_count = 10
};

enum raft_machine_state {
  raft_state_started = 0,
  raft_state_initial = 1,
  raft_state_leader_accept = 2,
  raft_state_recovery = 3,
  raft_state_wait_recovery = 4,
  raft_state_ready = 5
};

namespace boost {
  inline std::size_t hash_value(const boost::iterator_range<const uint8_t*>& v) {
    return boost::hash_range(v.begin(), v.end());
  }
}
struct wal_write_request;

struct raft_local_state {
  struct operation {
    operation(boost::asio::io_service& io_service)
      : gsn(0), server_id(0), lsn(0), submitted(0),rejected(0)
      , req(NULL), timeout(new boost::asio::deadline_timer(io_service))
    {}
    operation(operation&& op)
      : gsn(op.gsn), server_id(op.server_id), lsn(op.lsn), submitted(op.submitted)
      , rejected(op.rejected), key(op.key), req(op.req), timeout(op.timeout)
    { op.timeout = NULL; }

    operation(const operation& op)
      : gsn(op.gsn), server_id(op.server_id), lsn(op.lsn), submitted(op.submitted)
      , rejected(op.rejected), key(op.key), req(op.req), timeout(op.timeout)
    { op.timeout = NULL; }
    ~operation() { delete timeout; }

    uint64_t gsn;
    uint32_t server_id;
    uint64_t lsn;
    mutable uint32_t submitted;
    mutable uint32_t rejected;
    boost::iterator_range<const uint8_t*> key;
    wal_write_request* req;
    mutable boost::asio::deadline_timer* timeout;
  private:
    operation& operator=(const operation& op);
  };
  struct gsn_hash{};
  struct key_hash{};
  struct host_state_compare {
    bool operator()( const raft_host_state& lhs, const raft_host_state& rhs ) const {
      const raft_host_data& d1 = raft_host_index[lhs.server_id];
      const raft_host_data& d2 = raft_host_index[rhs.server_id];
      return d1.id == d2.id ? false : (d1.gsn > d2.gsn ? true : (d1.gsn < d2.gsn ? false : d1.id > d2.id));
    }
  };

private:
  typedef boost::multi_index_container<
      operation,
      boost::multi_index::indexed_by<
        boost::multi_index::ordered_unique<
          boost::multi_index::tag<gsn_hash>,
          BOOST_MULTI_INDEX_MEMBER(operation, uint64_t, gsn)>,
        boost::multi_index::hashed_unique<
          boost::multi_index::tag<key_hash>,
          BOOST_MULTI_INDEX_MEMBER(operation, boost::iterator_range<const uint8_t*>, key)>
      >
    > global_operation_map;
  typedef std::unordered_map<uint64_t, wal_write_request*> local_operation_map;
public:
  raft_local_state()
    : leader_id(-1), num_leader_accept(0), num_connected(1)
    , max_connected_id(-1), local_id(-1), state(raft_state_started)
    , lsn(-1), fiber_num(1), host_queue_len(1)
    , start_election_time(boost::posix_time::microsec_clock::universal_time())
  {}


  boost::asio::io_service io_service;

  std::set<raft_host_state, host_state_compare> host_state;
  int8_t leader_id;
  uint8_t num_leader_accept;
  uint8_t num_connected;
  int8_t max_connected_id;
  int8_t local_id;
  uint8_t state;
  int64_t lsn;
  uint32_t fiber_num;
  uint32_t host_queue_len;

  global_operation_map operation_index;
  local_operation_map local_operation_index;
  std::bitset<VCLOCK_MAX> hosts_for_recover;
  std::unordered_map<uint64_t, std::pair<uint32_t, uint64_t> > proxy_requests;

  /* settings */
  boost::posix_time::time_duration read_timeout;
  boost::posix_time::time_duration write_timeout;
  boost::posix_time::time_duration connect_timeout;
  boost::posix_time::time_duration resolve_timeout;
  boost::posix_time::time_duration reconnect_timeout;
  boost::posix_time::time_duration operation_timeout;
  boost::posix_time::ptime start_election_time;
};

extern struct raft_local_state raft_state;

inline bool has_consensus() {
  return raft_state.num_connected > raft_host_index.size() / 2;
}
inline bool has_leader() {
  return raft_state.leader_id >= 0;
}
inline bool is_leader() {
  return raft_state.leader_id == raft_state.local_id;
}

void raft_write_wal_remote(uint64_t gsn, uint32_t server_id);
void raft_write_wal_local_leader(const typename raft_local_state::operation& op);
void raft_write_wal_local_slave(uint64_t lsn, uint64_t gsn);
void raft_rollback_local(uint64_t gsn);
void raft_recover_node(int64_t gsn);
void raft_leader_promise();

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* TARANTOOL_RAFT_COMMON_H_INCLUDED */
