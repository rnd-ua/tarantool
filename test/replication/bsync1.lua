#!/usr/bin/env tarantool

box.cfg({
    log_level           = 6,
    listen              = os.getenv("LISTEN"),
    slab_alloc_arena    = 0.1,
    pid_file            = "tarantool.pid",
    logger              = "tarantool.log",

    bsync_enable        = 1,
    bsync_replica       = "127.0.0.1:9991;127.0.0.1:9992",
    bsync_local         = "127.0.0.1:9991",
})

require('console').listen(os.getenv('ADMIN'))
