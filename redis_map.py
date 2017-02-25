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
        return conn.hgetall(self.name).__iter__

    def items(self):
        conn = redis.StrictRedis(connection_pool=self.conn_pool)
        return conn.hgetall(self.name).items()
