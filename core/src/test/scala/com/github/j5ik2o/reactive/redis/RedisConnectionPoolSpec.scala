package com.github.j5ik2o.reactive.redis

import akka.routing.DefaultResizer
import cats.data.NonEmptyList
import monix.eval.Task

class RedisConnectionPoolSpec extends AbstractRedisConnectionPoolSpec("RedisConnectionPoolSpec") {
  override protected def createConnectionPool(peerConfigs: NonEmptyList[PeerConfig]): RedisConnectionPool[Task] =
    RedisConnectionPool.ofMultipleRoundRobin(
      sizePerPeer = 10,
      peerConfigs,
      RedisConnection.apply,
      reSizer = Some(DefaultResizer(lowerBound = 5, upperBound = 15))
    )
}
