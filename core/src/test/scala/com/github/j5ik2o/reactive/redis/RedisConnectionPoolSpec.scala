package com.github.j5ik2o.reactive.redis

import akka.routing.DefaultResizer
import cats.data.NonEmptyList
import monix.eval.Task

class RedisConnectionPoolSpec extends AbstractRedisConnectionPoolSpec("RedisConnectionPoolSpec") {
  override protected def createConnectionPool(peerConfigs: NonEmptyList[PeerConfig]): RedisConnectionPool[Task] = {
    val sizePerPeer = 2
    val lowerBound  = 1
    val upperBound  = 5
    val reSizer     = Some(DefaultResizer(lowerBound, upperBound))
    RedisConnectionPool.ofMultipleRoundRobin(
      sizePerPeer,
      peerConfigs,
      newConnection = RedisConnection.apply,
      reSizer = reSizer
    )
  }
}
