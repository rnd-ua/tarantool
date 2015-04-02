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
#include "memtx_engine.h"
#include "tuple.h"
#include "txn.h"
#include "index.h"
#include "memtx_hash.h"
#include "memtx_tree.h"
#include "memtx_rtree.h"
#include "memtx_bitset.h"
#include "space.h"
#include "salad/rlist.h"
#include <stdlib.h>
#include <string.h>
#include "box.h"
#include "iproto_constants.h"
#include "xrow.h"
#include "recovery.h"
#include "schema.h"
#include "tarantool.h"
#include "coeio_file.h"
#include "coio.h"
#include "errinj.h"
#include "scoped_guard.h"


/** For all memory used by all indexes. */
extern struct quota memtx_quota;
static bool memtx_index_arena_initialized = false;
static struct slab_arena memtx_index_arena;
static struct slab_cache memtx_index_arena_slab_cache;
static struct mempool memtx_index_extent_pool;


struct MemtxSpace: public Handler {
	MemtxSpace(Engine *e)
		: Handler(e)
	{ }
	virtual ~MemtxSpace()
	{
		/* do nothing */
		/* engine->close(this); */
	}
};

/**
 * This is a vtab with which a newly created space which has no
 * keys is primed.
 * At first it is set to correctly work for spaces created during
 * recovery from snapshot. In process of recovery it is updated as
 * below:
 *
 * 1) after SNAP is loaded:
 *    recover = space_build_primary_key
 * 2) when all XLOGs are loaded:
 *    recover = space_build_all_keys
*/

static inline void
memtx_recovery_prepare(struct engine_recovery *r)
{
	r->state   = READY_NO_KEYS;
	r->recover = space_begin_build_primary_key;
	r->replace = space_replace_no_keys;
}

MemtxEngine::MemtxEngine()
	:Engine("memtx"),
	m_snapshot_lsn(-1),
	m_snapshot_pid(0)
{
	flags = ENGINE_NO_YIELD |
	        ENGINE_CAN_BE_TEMPORARY |
		ENGINE_AUTO_CHECK_UPDATE;
	memtx_recovery_prepare(&recovery);
}

void
MemtxEngine::end_recover_snapshot()
{
	recovery.recover = space_build_primary_key;
}

void
MemtxEngine::end_recovery()
{
	recovery.recover = space_build_all_keys;
}

Handler *MemtxEngine::open()
{
	return new MemtxSpace(this);
}

Index *
MemtxEngine::createIndex(struct key_def *key_def)
{
	switch (key_def->type) {
	case HASH:
		return new MemtxHash(key_def);
	case TREE:
		return new MemtxTree(key_def);
	case RTREE:
		return new MemtxRTree(key_def);
	case BITSET:
		return new MemtxBitset(key_def);
	default:
		assert(false);
		return NULL;
	}
}

void
MemtxEngine::dropIndex(Index *index)
{
	struct iterator *it = index->position();
	index->initIterator(it, ITER_ALL, NULL, 0);
	struct tuple *tuple;
	while ((tuple = it->next(it)))
		tuple_unref(tuple);
}

void
MemtxEngine::keydefCheck(struct space *space, struct key_def *key_def)
{
	switch (key_def->type) {
	case HASH:
		if (! key_def->is_unique) {
			tnt_raise(ClientError, ER_MODIFY_INDEX,
				  key_def->name,
				  space_name(space),
				  "HASH index must be unique");
		}
		break;
	case TREE:
		/* TREE index has no limitations. */
		break;
	case RTREE:
		if (key_def->part_count != 1) {
			tnt_raise(ClientError, ER_MODIFY_INDEX,
				  key_def->name,
				  space_name(space),
				  "RTREE index key can not be multipart");
		}
		if (key_def->is_unique) {
			tnt_raise(ClientError, ER_MODIFY_INDEX,
				  key_def->name,
				  space_name(space),
				  "RTREE index can not be unique");
		}
		break;
	case BITSET:
		if (key_def->part_count != 1) {
			tnt_raise(ClientError, ER_MODIFY_INDEX,
				  key_def->name,
				  space_name(space),
				  "BITSET index key can not be multipart");
		}
		if (key_def->is_unique) {
			tnt_raise(ClientError, ER_MODIFY_INDEX,
				  key_def->name,
				  space_name(space),
				  "BITSET can not be unique");
		}
		break;
	default:
		tnt_raise(ClientError, ER_INDEX_TYPE,
			  key_def->name,
			  space_name(space));
		break;
	}
	for (uint32_t i = 0; i < key_def->part_count; i++) {
		switch (key_def->parts[i].type) {
		case NUM:
		case STRING:
			if (key_def->type == RTREE) {
				tnt_raise(ClientError, ER_MODIFY_INDEX,
					  key_def->name,
					  space_name(space),
					  "RTREE index field type must be ARRAY");
			}
			break;
		case ARRAY:
			if (key_def->type != RTREE) {
				tnt_raise(ClientError, ER_MODIFY_INDEX,
					  key_def->name,
					  space_name(space),
					  "ARRAY field type is not supported");
			}
			break;
		default:
			assert(false);
			break;
		}
	}
}

void
MemtxEngine::rollback(struct txn *txn)
{
	struct txn_stmt *stmt;
	rlist_foreach_entry_reverse(stmt, &txn->stmts, next) {
		if (stmt->old_tuple || stmt->new_tuple) {
			space_replace(stmt->space, stmt->new_tuple,
				      stmt->old_tuple, DUP_INSERT);
		}
	}
}

/** Called at start to tell memtx to recover to a given LSN. */
void
MemtxEngine::begin_recover_snapshot(int64_t /* lsn */)
{
	/*
	 * memtx snapshotting supported directly by box.
	 * do nothing here.
	 */
}

/** The snapshot row metadata repeats the structure of REPLACE request. */
struct request_replace_body {
	uint8_t m_body;
	uint8_t k_space_id;
	uint8_t m_space_id;
	uint32_t v_space_id;
	uint8_t k_tuple;
} __attribute__((packed));

static void
snapshot_write_row(struct recovery_state *r, struct xlog *l,
		   struct xrow_header *row)
{
	static uint64_t bytes;
	ev_tstamp elapsed;
	static ev_tstamp last = 0;
	ev_loop *loop = loop();

	row->tm = last;
	row->server_id = 0;
	/**
	 * Rows in snapshot are numbered from 1 to %rows.
	 * This makes streaming such rows to a replica or
	 * to recovery look similar to streaming a normal
	 * WAL. @sa the place which skips old rows in
	 * recovery_apply_row().
	 */
	row->lsn = ++l->rows;
	row->sync = 0; /* don't write sync to wal */

	struct iovec iov[XROW_IOVMAX];
	int iovcnt = xlog_encode_row(row, iov);

	/* TODO: use writev here */
	for (int i = 0; i < iovcnt; i++) {
		if (fwrite(iov[i].iov_base, iov[i].iov_len, 1, l->f) != 1) {
			say_error("Can't write row (%zu bytes)",
				  iov[i].iov_len);
			panic_syserror("snapshot_write_row");
		}
		bytes += iov[i].iov_len;
	}

	if (l->rows % 100000 == 0)
		say_crit("%.1fM rows written", l->rows / 1000000.);

	fiber_gc();

	if (r->snap_io_rate_limit != UINT64_MAX) {
		if (last == 0) {
			/*
			 * Remember the time of first
			 * write to disk.
			 */
			ev_now_update(loop);
			last = ev_now(loop);
		}
		/**
		 * If io rate limit is set, flush the
		 * filesystem cache, otherwise the limit is
		 * not really enforced.
		 */
		if (bytes > r->snap_io_rate_limit)
			fdatasync(fileno(l->f));
	}
	while (bytes > r->snap_io_rate_limit) {
		ev_now_update(loop);
		/*
		 * How much time have passed since
		 * last write?
		 */
		elapsed = ev_now(loop) - last;
		/*
		 * If last write was in less than
		 * a second, sleep until the
		 * second is reached.
		 */
		if (elapsed < 1)
			usleep(((1 - elapsed) * 1000000));

		ev_now_update(loop);
		last = ev_now(loop);
		bytes -= r->snap_io_rate_limit;
	}
}

static void
snapshot_write_tuple(struct xlog *l,
		     uint32_t n, struct tuple *tuple)
{
	struct request_replace_body body;
	body.m_body = 0x82; /* map of two elements. */
	body.k_space_id = IPROTO_SPACE_ID;
	body.m_space_id = 0xce; /* uint32 */
	body.v_space_id = mp_bswap_u32(n);
	body.k_tuple = IPROTO_TUPLE;

	struct xrow_header row;
	memset(&row, 0, sizeof(struct xrow_header));
	row.type = IPROTO_INSERT;

	row.bodycnt = 2;
	row.body[0].iov_base = &body;
	row.body[0].iov_len = sizeof(body);
	row.body[1].iov_base = tuple->data;
	row.body[1].iov_len = tuple->bsize;
	snapshot_write_row(recovery, l, &row);
}

/*
static void
snapshot_space(struct space *sp, void *udata)
{
	if (space_is_temporary(sp))
		return;
	if (space_is_sophia(sp))
		return;
	struct tuple *tuple;
	struct xlog *l = (struct xlog *)udata;
	Index *pk = space_index(sp, 0);
	if (pk == NULL)
		return;
	struct iterator *it = pk->position();
	pk->initIterator(it, ITER_ALL, NULL, 0);

	while ((tuple = it->next(it)))
		snapshot_write_tuple(l, space_id(sp), tuple);
}
*/

struct snap_space_data {
	int count;
	int alloc_count;
	struct iterator **iterators;
	struct space **spaces;
};

static void
snap_space_data_init(struct snap_space_data *data)
{
	data->count = 0;
	data->alloc_count = 0;
	data->iterators = 0;
	data->spaces = 0;
}

static void
snap_space_data_destroy(struct snap_space_data *data)
{
	for (int i = 0; i < data->count; i++) {
		data->iterators[i]->free(data->iterators[i]);
	}
	free(data->iterators);
	free(data->spaces);
	data->count = 0;
	data->alloc_count = 0;
	data->iterators = 0;
	data->spaces = 0;
}

static void
snap_space_data_add_space(struct space *sp, void *udata)
{
	if (space_is_temporary(sp))
		return;
	if (space_is_sophia(sp))
		return;
	struct snap_space_data *data = (struct snap_space_data *)udata;
	if (data->count == data->alloc_count) {
		int new_alloc_count = data->alloc_count * 2 + 1;
		data->iterators = (struct iterator **)
			realloc(data->iterators, new_alloc_count * sizeof(struct iterator *));
		data->spaces = (struct space **)
			realloc(data->spaces, new_alloc_count * sizeof(struct space *));
		/* TODO: check realloc result */
		data->alloc_count = new_alloc_count;
	}
	data->spaces[data->count] = sp;
	Index *pk = space_index(sp, 0);
	data->iterators[data->count] = pk->allocIterator();
	data->count++;
	pk->initIterator(data->iterators[data->count - 1], ITER_ALL, NULL, 0);
	pk->freezeIterator(data->iterators[data->count - 1]);
}

static void
snap_space_data_snapshot_all(struct snap_space_data *data, struct xlog *l)
{
	struct tuple *tuple;
	for (int i = 0; i < data->count; i++) {
		while ((tuple = data->iterators[i]->next(data->iterators[i])))
			snapshot_write_tuple(l, space_id(data->spaces[i]), tuple);
	}
}

static void
snapshot_save(struct recovery_state *r)
{
	struct xlog *snap = xlog_create(&r->snap_dir, &r->vclock);
	if (snap == NULL)
		panic_status(errno, "failed to save snapshot: failed to open file in write mode.");
	/*
	 * While saving a snapshot, snapshot name is set to
	 * <lsn>.snap.inprogress. When done, the snapshot is
	 * renamed to <lsn>.snap.
	 */
	say_info("saving snapshot `%s'", snap->filename);

	struct snap_space_data snap_space_data;
	snap_space_data_init(&snap_space_data);
	auto scoped_guard = make_scoped_guard([&] { snap_space_data_destroy(&snap_space_data); });
	space_foreach(snap_space_data_add_space, &snap_space_data);
	snap_space_data_snapshot_all(&snap_space_data, snap);

	/** suppress rename */
	snap->is_inprogress = false;
	xlog_close(snap);

	say_info("done");
}

int
MemtxEngine::begin_checkpoint(int64_t lsn)
{
	assert(m_snapshot_lsn == -1);
	assert(m_snapshot_pid == 0);
	/* flush buffers to avoid multiple output
	 *
	 * https://github.com/tarantool/tarantool/issues/366
	*/
	fflush(stdout);
	fflush(stderr);
	/*
	 * Due to fork nature, no threads are recreated.
	 * This is the only consistency guarantee here for a
	 * multi-threaded engine.
	 */
	snapshot_pid = fork();

	switch (snapshot_pid) {
	case -1:
		say_syserror("fork");
		return -1;
	case  0: /* dumper */
		slab_arena_mprotect(&memtx_arena);
		cord_set_name("snap");
		title("dumper", "%" PRIu32, getppid());
		fiber_set_name(fiber(), "dumper");
		/* make sure we don't double-write parent stdio
		 * buffers at exit() during panic */
		close_all_xcpt(1, log_fd);
		/* do not rename snapshot */
		snapshot_save(::recovery);
		exit(EXIT_SUCCESS);
		return 0;
	default: /* waiter */
		break;
	}

	m_snapshot_lsn = lsn;
	m_snapshot_pid = snapshot_pid;
	/* increment snapshot version */
	tuple_begin_snapshot();
	return 0;
}

int
MemtxEngine::wait_checkpoint()
{
	assert(m_snapshot_lsn >= 0);
	assert(m_snapshot_pid > 0);
	/* wait for memtx-part snapshot completion */
	int rc = coio_waitpid(m_snapshot_pid);
	if (WIFSIGNALED(rc))
		rc = EINTR;
	else
		rc = WEXITSTATUS(rc);
	/* complete snapshot */
	tuple_end_snapshot();
	m_snapshot_pid = snapshot_pid = 0;
	errno = rc;

	return rc;
}

void
MemtxEngine::commit_checkpoint()
{
	/* begin_checkpoint() must have been done */
	assert(m_snapshot_lsn >= 0);
	/* wait_checkpoint() must have been done. */
	assert(m_snapshot_pid == 0);

	struct xdir *dir = &::recovery->snap_dir;
	/* rename snapshot on completion */
	char to[PATH_MAX];
	snprintf(to, sizeof(to), "%s",
		 format_filename(dir, m_snapshot_lsn, NONE));
	char *from = format_filename(dir, m_snapshot_lsn, INPROGRESS);
	int rc = coeio_rename(from, to);
	if (rc != 0)
		panic("can't rename .snap.inprogress");

	m_snapshot_lsn = -1;
}

void
MemtxEngine::abort_checkpoint()
{
	if (m_snapshot_pid > 0) {
		assert(m_snapshot_lsn >= 0);
		/**
		 * An error in the other engine's first phase.
		 */
		wait_checkpoint();
	}
	if (m_snapshot_lsn > 0) {
		/** Remove garbage .inprogress file. */
		char *filename = format_filename(&::recovery->snap_dir,
						m_snapshot_lsn, INPROGRESS);
		(void) coeio_unlink(filename);
	}
	m_snapshot_lsn = -1;
}

/**
 * Initialize arena for indexes.
 * The arena is used for memtx_index_extent_alloc
 *  and memtx_index_extent_free.
 * Can be called several times, only first call do the work.
 */
void
memtx_index_arena_init()
{
	if (memtx_index_arena_initialized) {
		/* already done.. */
		return;
	}
	/* Creating arena */
	if (slab_arena_create(&memtx_index_arena, &memtx_quota,
			      0, MEMTX_SLAB_SIZE, MAP_PRIVATE)) {
		panic_syserror("failed to initialize index arena");
	}
	/* Creating slab cache */
	slab_cache_create(&memtx_index_arena_slab_cache, &memtx_index_arena);
	/* Creating mempool */
	mempool_create(&memtx_index_extent_pool,
		       &memtx_index_arena_slab_cache,
		       MEMTX_EXTENT_SIZE);
	/* Done */
	memtx_index_arena_initialized = true;
}

/**
 * Allocate a block of size MEMTX_EXTENT_SIZE for memtx index
 */
void *
memtx_index_extent_alloc()
{
	ERROR_INJECT(ERRINJ_INDEX_ALLOC, return 0);
	return mempool_alloc(&memtx_index_extent_pool);
}

/**
 * Free a block previously allocated by memtx_index_extent_alloc
 */
void
memtx_index_extent_free(void *extent)
{
	return mempool_free(&memtx_index_extent_pool, extent);
}
