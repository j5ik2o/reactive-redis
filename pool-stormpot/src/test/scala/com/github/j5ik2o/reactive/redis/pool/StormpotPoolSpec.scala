package com.github.j5ik2o.reactive.redis.pool

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.{
  AbstractRedisConnectionPoolSpec,
  PeerConfig,
  RedisConnection,
  RedisConnectionPool
}
import monix.eval.Task

class StormpotPoolSpec extends AbstractRedisConnectionPoolSpec("StormpotPoolSpec") {
  override protected def createConnectionPool(connectionConfigs: NonEmptyList[PeerConfig]): RedisConnectionPool[Task] =
    StormpotPool.ofMultiple(StormpotConfig(), connectionConfigs, RedisConnection.apply)
}
