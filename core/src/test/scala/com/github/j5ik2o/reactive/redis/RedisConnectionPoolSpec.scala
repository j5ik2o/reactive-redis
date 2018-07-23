package com.github.j5ik2o.reactive.redis

import akka.routing.DefaultResizer
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class RedisConnectionPoolSpec extends AbstractRedisConnectionPoolSpec("RedisConnectionPoolSpec") {
  override protected def createConnectionPool(peerConfigs: Seq[PeerConfig]): RedisConnectionPool[Task] =
    RedisConnectionPool.ofRoundRobin(sizePerPeer = 10,
                                     peerConfigs,
                                     RedisConnection(_, _),
                                     resizer = Some(DefaultResizer(lowerBound = 5, upperBound = 15)))
}
