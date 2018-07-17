package com.github.j5ik2o.reactive.redis.pool

import com.github.j5ik2o.reactive.redis.{ PeerConfig, RedisConnectionPool, RedisConnectionPoolSpec }
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class FOPPoolSpec extends RedisConnectionPoolSpec {
  override protected def createConnectionPool(connectionConfigs: Seq[PeerConfig]): RedisConnectionPool[Task] =
    FOPPool[Task](FOPConfig(), connectionConfigs)
}
