-- box.info().sophia['sophia.version']

space = box.schema.space.create('test', { engine = 'sophia' })
index = space:create_index('primary', { type = 'tree', parts = {1, 'num'} })

for key = 1, 10 do space:insert({key}) end

-- box.info().sophia['db.100.index.count']

space:drop()
