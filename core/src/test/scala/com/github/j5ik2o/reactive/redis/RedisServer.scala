package com.github.j5ik2o.reactive.redis

import redis.embedded.RedisExecProvider

class RedisServer(redisExecProvider: RedisExecProvider, port: Int)
    extends redis.embedded.RedisServer(redisExecProvider, port) {
  override def redisReadyPattern(): String = ".*Ready to accept connections"
}
