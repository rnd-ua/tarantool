#ifndef TARANTOOL_RAFT_ZCOP_BUFF_H_INCLUDED
#define TARANTOOL_RAFT_ZCOP_BUFF_H_INCLUDED
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
#include <boost/iterator.hpp>

template <typename T = boost::asio::const_buffer, typename Allocator = std::allocator<T> >
class raft_zcopy_iterator
  : public boost::iterator_facade<
        raft_zcopy_iterator<T, Allocator>
      , boost::asio::const_buffer const
      , boost::forward_traversal_tag
    >
{
  typedef typename std::list<T, Allocator>::const_iterator queue_iterator;
 public:
    explicit raft_zcopy_iterator(queue_iterator node, queue_iterator node_end)
      : node_(node), node_end_(node_end), end_(false)
    {}
    raft_zcopy_iterator() : end_(true) {}

 private:
    friend class boost::iterator_core_access;

    void increment() { ++node_; end_ = (node_ == node_end_); }

    bool equal(raft_zcopy_iterator const& other) const
    {
        return (this->node_ == other.node_) || (this->end_ && other.end_);
    }

    const boost::asio::const_buffer& dereference() const { return *node_; }

    queue_iterator node_;
    queue_iterator node_end_;
    bool end_;
};

template <typename T, typename Allocator>
class basic_raft_zcopy_buff {
  basic_raft_zcopy_buff(std::list<T, Allocator>&& l, std::size_t s)
    : queue_(std::move(l)), size_(s)
  {}
public:
  typedef raft_zcopy_iterator<T, Allocator> const_iterator;

  basic_raft_zcopy_buff() : size_(0) {}

  const_iterator begin() const {
    return const_iterator(queue_.begin(), queue_.end());
  }

  const_iterator end() const {
    return const_iterator();
  }

  basic_raft_zcopy_buff split() {
    std::list<T, Allocator> queue;
    queue.splice(queue.begin(), queue_);
    std::size_t s = size_;
    size_ = 0;
    return basic_raft_zcopy_buff(std::move(queue), s);
  }

  void push(T v) { queue_.emplace_back(v); size_ += boost::asio::buffer_size(queue_.back()); }

  std::size_t size() const {
    return size_;
  }

  void clear() {
    for (auto& i : queue_) free(const_cast<void*>(boost::asio::buffer_cast<const void*>(i)));
    queue_.clear();
    size_ = 0;
  }

private:
  std::list<T, Allocator> queue_;
  std::size_t size_;
};

typedef basic_raft_zcopy_buff<boost::asio::const_buffer, std::allocator<boost::asio::const_buffer> > raft_zcopy_buff;

#endif /* TARANTOOL_RAFT_ZCOP_BUFF_H_INCLUDED */
