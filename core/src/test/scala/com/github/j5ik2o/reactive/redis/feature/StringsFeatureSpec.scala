package com.github.j5ik2o.reactive.redis.feature

import akka.actor.ActorSystem
import akka.routing.DefaultResizer
import cats.implicits._
import com.github.j5ik2o.reactive.redis.{ AbstractRedisClientSpec, PeerConfig, RedisConnection, RedisConnectionPool }
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Shrink

class StringsFeatureSpec extends AbstractRedisClientSpec(ActorSystem("StringsFeatureSpec")) {
  implicit val noShrink: Shrink[String] = Shrink.shrinkAny

  override protected def createConnectionPool(peerConfigs: Seq[PeerConfig]): RedisConnectionPool[Task] =
    RedisConnectionPool.ofRoundRobin(sizePerPeer = 10,
                                     peerConfigs,
                                     RedisConnection(_, _),
                                     resizer = Some(DefaultResizer(lowerBound = 5, upperBound = 15)))

  "StringsFeature" - {
    "set & get" in forAll(keyValueGen) {
      case (k, v) =>
        val result = runProgram(for {
          _      <- redisClient.set(k, v)
          result <- redisClient.get(k)
        } yield result)

        result.value should not be empty
    }
  }

}
