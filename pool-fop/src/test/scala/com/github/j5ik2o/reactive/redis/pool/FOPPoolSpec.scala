package com.github.j5ik2o.reactive.redis.pool

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.{
  AbstractRedisConnectionPoolSpec,
  PeerConfig,
  RedisConnection,
  RedisConnectionPool
}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class FOPPoolSpec extends AbstractRedisConnectionPoolSpec("FOPPoolSpec") {
  override protected def createConnectionPool(connectionConfigs: NonEmptyList[PeerConfig]): RedisConnectionPool[Task] =
    FOPPool(FOPConfig(), connectionConfigs, RedisConnection(_, _))
}
