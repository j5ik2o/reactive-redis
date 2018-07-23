package com.github.j5ik2o.reactive.redis.feature

import akka.actor.ActorSystem
import akka.routing.DefaultResizer
import com.github.j5ik2o.reactive.redis.{ AbstractRedisClientSpec, PeerConfig, RedisConnection, RedisConnectionPool }
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Shrink

class HashesFeatureSpec extends AbstractRedisClientSpec(ActorSystem("HashesFeatureSpec")) {

  implicit val noShrink: Shrink[String] = Shrink.shrinkAny

  override protected def createConnectionPool(peerConfigs: Seq[PeerConfig]): RedisConnectionPool[Task] =
    RedisConnectionPool.ofRoundRobin(sizePerPeer = 10,
                                     peerConfigs,
                                     RedisConnection(_, _),
                                     resizer = Some(DefaultResizer(lowerBound = 5, upperBound = 15)))

  "HashesFeature" - {
    "hdel" in forAll(keyFieldValueGen) {
      case (k, f, v) =>
        val result1 = runProgram(for {
          _ <- redisClient.hset(k, f, v)
          r <- redisClient.hget(k, f)
        } yield r)
        result1.value shouldBe Some(v)
        val result2 = runProgram(for {
          n <- redisClient.hdel(k, f)
          r <- redisClient.hget(k, f)
        } yield (n, r))
        result2._1.value shouldBe 1
        result2._2.value shouldBe None
    }
    "hset & hget" in forAll(keyFieldValueGen) {
      case (k, f, v) =>
        val result = runProgram(for {
          _ <- redisClient.hset(k, f, v)
          r <- redisClient.hget(k, f)
        } yield r)
        result.value shouldBe Some(v)
    }
    "hsetnx & hget" in forAll(keyFieldValueGen) {
      case (k, f, v) =>
        val result = runProgram(for {
          r1 <- redisClient.hsetNx(k, f, v)
          v1 <- redisClient.hget(k, f)
          r2 <- redisClient.hsetNx(k, f, "(" + v + ")")
          v2 <- redisClient.hget(k, f)
        } yield (r1, r2, v1, v2))
        result._1.value shouldBe true
        result._2.value shouldBe false
        result._3.value shouldBe Some(v)
        result._4.value shouldBe Some(v)
    }
    "hgetall" in forAll(keyFieldValueGen) {
      case (k, f, v) =>
        val result = runProgram(for {
          _ <- redisClient.hset(k, f, v)
          r <- redisClient.hgetAll(k)
        } yield r)
        result.value(0) shouldBe f
        result.value(1) shouldBe v
    }
  }

}
