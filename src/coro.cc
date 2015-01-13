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
#include "coro.h"

#include "trivia/config.h"
#include "exception.h"
#include "tt_pthread.h"
#include <unistd.h>
#include <string.h>
#include <sys/mman.h>

#include "third_party/valgrind/memcheck.h"
#include "fiber.h"

static pthread_mutex_t coro_mutex;

void
tarantool_coro_init()
{
	pthread_mutexattr_t errorcheck;
	(void) tt_pthread_mutexattr_init(&errorcheck);
#ifndef NDEBUG
	(void) tt_pthread_mutexattr_settype(&errorcheck, PTHREAD_MUTEX_ERRORCHECK);
#endif
	tt_pthread_mutex_init(&coro_mutex, &errorcheck);
}

void
tarantool_coro_stop()
{
	tt_pthread_mutex_destroy(&coro_mutex);
}

void
tarantool_coro_create(struct tarantool_coro *coro,
		      struct slab_cache *slabc,
		      void (*f) (void *), void *data)
{
	const int page = sysconf(_SC_PAGESIZE);

	memset(coro, 0, sizeof(*coro));

	/* TODO: guard pages */
	coro->stack_size = page * 16 - slab_sizeof();
	coro->stack = (char *) slab_get(slabc, coro->stack_size)
					+ slab_sizeof();

	if (coro->stack == NULL) {
		tnt_raise(OutOfMemory, sizeof(coro->stack_size),
			  "mmap", "coro stack");
	}

	(void) VALGRIND_STACK_REGISTER(coro->stack, (char *)
				       coro->stack + coro->stack_size);

	tt_pthread_mutex_lock(&coro_mutex);
	coro_create(&coro->ctx, f, data, coro->stack, coro->stack_size);
	tt_pthread_mutex_unlock(&coro_mutex);
}

void
tarantool_coro_destroy(struct tarantool_coro *coro, struct slab_cache *slabc)
{
	if (coro->stack != NULL) {
		slab_put(slabc, (struct slab *)
			 ((char *) coro->stack - slab_sizeof()));
	}
}
