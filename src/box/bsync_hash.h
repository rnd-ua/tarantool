#ifndef TARANTOOL_BOX_BSYNC_HASH_H_INCLUDED
#define TARANTOOL_BOX_BSYNC_HASH_H_INCLUDED
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
#include <stdint.h>
#include "crc32.h"
#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

#if !MH_SOURCE
#define MH_UNDEF
#endif

struct bsync_key {
	const void *data;
	size_t size;
};
#if MH_SOURCE
static uint32_t bsync_hash_key(const struct bsync_key *key) {
	return crc32_calc(0, (const char *)key->data, key->size);
}
#endif

/*
 * Map: (const char *) => (void *)
 */
#define mh_name _strptr
#define mh_key_t struct bsync_key
struct mh_strptr_node_t {
	mh_key_t key;
	void *val;
};

#define mh_node_t struct mh_strptr_node_t
#define mh_arg_t void *
#define mh_hash(a, arg) (bsync_hash_key(&(a)->key))
#define mh_hash_key(a, arg) (bsync_hash_key(&(a)))
#define mh_eq(a, b, arg) ((a)->key.size != (b)->key.size ? 0 \
			  : !memcmp((a)->key.data, (b)->key.data, (a)->key.size))
#define mh_eq_key(a, b, arg) ((a).size != (b)->key.size ? 0 \
			  : !memcmp((a).data, (b)->key.data, (a).size))
#include "salad/mhash.h"

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* TARANTOOL_BOX_BSYNC_HASH_H_INCLUDED */
