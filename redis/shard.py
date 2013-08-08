#! /usr/bin/env python
import random
from redis.connection import BlockingConnectionPool
from redis.client     import StrictRedis


class RedisShard(object):
  def __init__(self, servers_for_write, servers_for_read):
    self._normalize_server_args(servers_for_write)
    self._normalize_server_args(servers_for_read)
    self._w_client = self._make_client(servers_for_write)
    self._r_client = self._make_client(servers_for_read)

  def _normalize_server_args(self, server_info):
    assert server_info.setdefault("max_connections", 1) >= 1
    assert server_info.setdefault("timeout", 1)  >= 1 # connection pool lock timeout
    assert server_info.setdefault("socket_timeout", 3) >= 1
    assert server_info.has_key("alternate_hosts")

  def _make_client(self, info):
    pool = BlockingConnectionPool(**info)
    return StrictRedis(connection_pool = pool)

  def get_r_client(self):
    return self._r_client

  def get_w_client(self):
    return self._w_client

class RedisShardingProxy(object):
  def __init__(self, shard_func):
    self._shards = {}
    self._shard_calc = shard_func # shard_calc(key) : return shard_name

  def add_shard(self, name, **shard_args):
    self._shards[name] = RedisShard(**shard_args)

  def get_shard(self, key):
    return self._shards[self._shard_calc(key)]

  def set(self, key, value):
    shard = self.get_shard(key)
    cli = shard.get_w_client()
    return cli.set(key, value)

  def get(self, key):
    shard = self.get_shard(key)
    cli = shard.get_r_client()
    return cli.get(key)

if __name__ == "__main__":
  proxy = RedisShardingProxy(lambda k: k)

  shard1 = {
    "name":"key1",
    "servers_for_read": {
      "alternate_hosts": [{"host":"localhost", "port":8379}]
    },
    "servers_for_write": {
      "alternate_hosts": [{"host":"localhost", "port":8379}]
    }
  }
  proxy.add_shard(**shard1)
  shard2 = {
    "name":"key2",
    "servers_for_read": {
      "alternate_hosts": [{"host":"localhost", "port":7379}]
    },
    "servers_for_write": {
      "alternate_hosts": [{"host":"localhost", "port":7379}]
    }
  }

  proxy.add_shard(**shard2)

  proxy.set("key1", "xiao")
  assert proxy.get("key1") == "xiao"
  proxy.set("key2", "kyle")
  assert proxy.get("key2") == "kyle"



