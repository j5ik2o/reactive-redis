package com.github.j5ik2o.reactive.redis.feature

import akka.actor.ActorSystem
import akka.routing.DefaultResizer
import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.{ AbstractRedisClientSpec, PeerConfig, RedisConnection, RedisConnectionPool }
import monix.eval.Task
import org.scalacheck.Shrink

class ConnectionFeatureSpec extends AbstractRedisClientSpec(ActorSystem("ConnectionFeatureSpec")) {
  implicit val noShrink: Shrink[String] = Shrink.shrinkAny

  override protected def createConnectionPool(peerConfigs: NonEmptyList[PeerConfig]): RedisConnectionPool[Task] =
    RedisConnectionPool.ofMultipleRoundRobin(sizePerPeer = 10,
                                             peerConfigs,
                                             RedisConnection(_, _),
                                             reSizer = Some(DefaultResizer(lowerBound = 5, upperBound = 15)))
  "ConnectionFeatureSpec" - {
    "ping" in forAll(keyStrValueGen) {
      case (_, value) =>
        val result = runProgram(for {
          r <- redisClient.ping(Some(value))
        } yield r)
        result.value shouldBe value
    }
  }
}
