package com.github.j5ik2o.reactive.redis.pool

import com.github.j5ik2o.reactive.redis.{ AbstractRedisConnectionPoolSpec, PeerConfig, RedisConnectionPool }
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class ScalaPoolSpec extends AbstractRedisConnectionPoolSpec("ScalaPoolSpec") {
  override protected def createConnectionPool(connectionConfigs: Seq[PeerConfig]): RedisConnectionPool[Task] =
    ScalaPool[Task](ScalaPoolConfig(), connectionConfigs)
}
