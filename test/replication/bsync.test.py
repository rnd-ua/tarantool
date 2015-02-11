import os
import glob
from lib.tarantool_server import TarantoolServer

bsync1 = TarantoolServer(server.ini)
bsync1.script = 'replication/bsync1.lua'
bsync1.vardir = os.path.join(server.vardir, 'bsync1')
bsync1.deploy()

bsync2 = TarantoolServer(server.ini)
bsync2.script = 'replication/bsync2.lua'
bsync2.vardir = os.path.join(server.vardir, 'bsync2')
bsync2.deploy()

bsync1.admin("box.schema.user.grant('guest', 'read,write,execute', 'universe')")
bsync1.admin("space = box.schema.create_space('test', {id =  42})")
bsync1.admin("space:create_index('primary', { type = 'tree', parts = {1, 'NUM'}})")

bsync1.admin('for k = 0, 9 do space:insert{k, k*k} end')
bsync2.admin('for k = 10, 19 do box.space.test:insert{k, k*k} end')

bsync1.admin('box.space.test:select()')
bsync2.admin('box.space.test:select()')

bsync1.stop()
bsync1.cleanup(True)

bsync2.stop()
bsync2.cleanup(True)
