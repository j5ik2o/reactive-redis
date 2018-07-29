package com.github.j5ik2o.reactive.redis.pool

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.{
  AbstractRedisConnectionPoolSpec,
  PeerConfig,
  RedisConnection,
  RedisConnectionPool
}
import monix.eval.Task

class FOPPoolSpec extends AbstractRedisConnectionPoolSpec("FOPPoolSpec") {
  override protected def createConnectionPool(connectionConfigs: NonEmptyList[PeerConfig]): RedisConnectionPool[Task] =
    FOPPool.ofMultiple(FOPConfig(), connectionConfigs, RedisConnection(_, _))
}
