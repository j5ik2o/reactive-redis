package com.github.j5ik2o.reactive.redis.feature

import akka.actor.ActorSystem
import akka.routing.DefaultResizer
import cats.data.NonEmptyList
import cats.implicits._
import com.github.j5ik2o.reactive.redis.{ AbstractRedisClientSpec, PeerConfig, RedisConnection, RedisConnectionPool }
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Shrink

import scala.concurrent.duration._

class ListsFeatureSpec extends AbstractRedisClientSpec(ActorSystem("ListsFeatureSpec")) {
  implicit val noShrink: Shrink[String] = Shrink.shrinkAny

  override protected def createConnectionPool(peerConfigs: NonEmptyList[PeerConfig]): RedisConnectionPool[Task] =
    RedisConnectionPool.ofMultipleRoundRobin(sizePerPeer = 10,
                                             peerConfigs,
                                             RedisConnection(_, _),
                                             reSizer = Some(DefaultResizer(lowerBound = 5, upperBound = 15)))

  "ListsFeature" - {
    "lpush & blpop" in forAll(keyStrValuesGen) {
      case (k, values) =>
        val result = runProgram(for {
          _ <- redisClient.lpush(k, NonEmptyList(values.head, values.tail))
          r <- redisClient.blpop(NonEmptyList.of(k), 1 seconds)
        } yield r)
        result.value(1) shouldBe values.last
    }

    "lpush & lpop" in forAll(keyStrValuesGen) {
      case (k, values) =>
        val result = runProgram(for {
          _ <- redisClient.lpush(k, NonEmptyList(values.head, values.tail))
          r <- redisClient.lpop(k)
        } yield r)
        result.value.get shouldBe values.last
    }
  }

}
