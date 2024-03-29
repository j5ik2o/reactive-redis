package com.github.j5ik2o.reactive.redis.feature

import akka.routing.DefaultResizer
import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis._
import monix.eval.Task

class HashesFeatureOfDefaultSpec extends AbstractHashesFeatureSpec {

  override protected def createConnectionPool(peerConfigs: NonEmptyList[PeerConfig]): RedisConnectionPool[Task] = {
    val sizePerPeer = 2
    val lowerBound  = 1
    val upperBound  = 5
    val reSizer     = Some(DefaultResizer(lowerBound, upperBound))
    RedisConnectionPool.ofMultipleRoundRobin(
      sizePerPeer,
      peerConfigs,
      newConnection = RedisConnection.ofDefault,
      reSizer = reSizer
    )
  }

}
