package com.github.j5ik2o.reactive.redis.feature

import akka.actor.ActorSystem
import akka.routing.DefaultResizer
import cats.data.NonEmptyList
import cats.implicits._
import com.github.j5ik2o.reactive.redis.{ AbstractRedisClientSpec, PeerConfig, RedisConnection, RedisConnectionPool }
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Shrink

class ListsFeatureSpec extends AbstractRedisClientSpec(ActorSystem("ListsFeatureSpec")) {
  implicit val noShrink: Shrink[String] = Shrink.shrinkAny

  override protected def createConnectionPool(peerConfigs: Seq[PeerConfig]): RedisConnectionPool[Task] =
    RedisConnectionPool.ofRoundRobin(sizePerPeer = 10,
                                     peerConfigs,
                                     RedisConnection(_, _),
                                     resizer = Some(DefaultResizer(lowerBound = 5, upperBound = 15)))

  "ListsFeature" - {
    "lpush & lpop" in forAll(keyValuesGen) {
      case (k, values) =>
        val result = runProgram(for {
          _ <- redisClient.lpush(k, NonEmptyList(values.head, values.tail))
          r <- redisClient.lpop(k)
        } yield r)
        result.value.get shouldBe values.last
    }
  }

}
