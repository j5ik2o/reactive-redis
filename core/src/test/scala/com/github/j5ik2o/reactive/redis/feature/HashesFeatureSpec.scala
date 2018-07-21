package com.github.j5ik2o.reactive.redis.feature

import akka.actor.ActorSystem
import akka.routing.DefaultResizer
import com.github.j5ik2o.reactive.redis.{ AbstractRedisClientSpec, PeerConfig, RedisConnection, RedisConnectionPool }
import monix.eval.Task
import org.scalacheck.Shrink
import monix.execution.Scheduler.Implicits.global

class HashesFeatureSpec extends AbstractRedisClientSpec(ActorSystem("HashesFeatureSpec")) {

  implicit val noShrink: Shrink[String] = Shrink.shrinkAny

  override protected def createConnectionPool(peerConfigs: Seq[PeerConfig]): RedisConnectionPool[Task] =
    RedisConnectionPool.ofRoundRobin(sizePerPeer = 10, peerConfigs, newConnection = {
      RedisConnection(_)
    }, resizer = Some(DefaultResizer(lowerBound = 5, upperBound = 15)))

  "HashesFeature" - {
    "hset & hget" in forAll(keyFieldValueGen) {
      case (k, f, v) =>
        val result = runProgram(for {
          _ <- redisClient.hset(k, f, v)
          r <- redisClient.hget(k, f)
        } yield r)
        result.value shouldBe v
    }
  }

}
