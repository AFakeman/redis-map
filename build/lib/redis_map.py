import redis

class RedisMap:
    def __init__(self, host=None, port=None, name=None, conn_pool=None, db=0):
        if not conn_pool:
            self.conn_pool = redis.ConnectionPool(host=host, port=port, db=db)
        else:
            self.conn_pool = conn_pool
        self.name = name

    def __getitem__(self, item):
        conn = redis.StrictRedis(connection_pool=self.conn_pool)
        return conn.hget(self.name, item)

    def __setitem__(self, key, value):
        conn = redis.StrictRedis(connection_pool=self.conn_pool)
        conn.hset(self.name, key, value)

    def __contains__(self, item):
        conn = redis.StrictRedis(connection_pool=self.conn_pool)
        return conn.hexists(self.name, key=item)

    def __str__(self):
        conn = redis.StrictRedis(connection_pool=self.conn_pool)
        return str(conn.hgetall(self.name))

    def __iter__(self):
        conn = redis.StrictRedis(connection_pool=self.conn_pool)
        return conn.hgetall(self.name).__iter__()

    def __delitem__(self, key):
        conn = redis.StrictRedis(connection_pool=self.conn_pool)
        conn.hdel(self.name, key)

    def items(self):
        conn = redis.StrictRedis(connection_pool=self.conn_pool)
        return conn.hgetall(self.name).items()


class RedisUniterableMap:
    def __init__(self, host=None, port=None, base_key=None, conn_pool=None, db=0):
        if not conn_pool:
            self.conn_pool = redis.ConnectionPool(host=host, port=port, db=db)
        else:
            self.conn_pool = conn_pool
        self.base_key = base_key

    def __getitem__(self, item):
        conn = redis.StrictRedis(connection_pool=self.conn_pool)
        item_key = "{0}:{1}".format(self.base_key, item)
        item_type = conn.type(item_key).decode('ascii')
        if item_type == "string":
            return conn.get(item_key)
        elif item_type == "hash":
            return RedisMap(name=item_key, conn_pool=self.conn_pool)
        else:
            raise NotImplementedError(item_type)

    def __contains__(self, item):
        conn = redis.StrictRedis(connection_pool=self.conn_pool)
        item_key = "{0}:{1}".format(self.base_key, item)
        return conn.exists(item_key)

    def __setitem__(self, key, value):
        conn = redis.StrictRedis(connection_pool=self.conn_pool)
        item_key = "{0}:{1}".format(self.base_key, key)
        conn.delete(item_key)
        if isinstance(value, str):
            conn.set(item_key, value)
        elif isinstance(value, dict):
            conn.hmset(item_key, value)
        elif isinstance(value, set):
            conn.sadd(item_key, value)
        else:
            conn.lpush(item_key, value)

    def __delitem__(self, key):
        conn = redis.StrictRedis(connection_pool=self.conn_pool)
        item_key = "{0}:{1}".format(self.base_key, key)
        conn.delete(item_key)