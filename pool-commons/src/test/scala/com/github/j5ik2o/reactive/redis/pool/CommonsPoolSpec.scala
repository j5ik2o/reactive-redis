package com.github.j5ik2o.reactive.redis.pool

import com.github.j5ik2o.reactive.redis.{
  AbstractRedisConnectionPoolSpec,
  PeerConfig,
  RedisConnection,
  RedisConnectionPool
}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class CommonsPoolSpec extends AbstractRedisConnectionPoolSpec("CommonsPoolSpec") {
  override protected def createConnectionPool(connectionConfigs: Seq[PeerConfig]): RedisConnectionPool[Task] =
    CommonsPool(CommonsPoolConfig(), connectionConfigs, RedisConnection(_, _))
}
