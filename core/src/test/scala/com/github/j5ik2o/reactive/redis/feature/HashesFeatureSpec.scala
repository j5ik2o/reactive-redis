package com.github.j5ik2o.reactive.redis.feature

import akka.actor.ActorSystem
import akka.routing.DefaultResizer
import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis._
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Shrink

class HashesFeatureSpec extends AbstractRedisClientSpec(ActorSystem("HashesFeatureSpec")) {

  implicit val noShrink: Shrink[String] = Shrink.shrinkAny

  override protected def createConnectionPool(peerConfigs: NonEmptyList[PeerConfig]): RedisConnectionPool[Task] =
    RedisConnectionPool.ofMultipleRoundRobin(sizePerPeer = 10,
                                             peerConfigs,
                                             RedisConnection(_, _),
                                             reSizer = Some(DefaultResizer(lowerBound = 5, upperBound = 15)))

  "HashesFeature" - {
    "hdel" in forAll(keyFieldStrValueGen) {
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
    "hexists" in forAll(keyFieldStrValueGen) {
      case (k, f, v) =>
        val result = runProgram(for {
          _  <- redisClient.hset(k, f, v)
          r1 <- redisClient.hexists(k, f)
          _  <- redisClient.hdel(k, f)
          r2 <- redisClient.hexists(k, f)
        } yield (r1, r2))
        result shouldBe (Provided(true), Provided(false))
    }
    "hget" in forAll(keyFieldStrValueGen) {
      case (k, f, v) =>
        val result = runProgram(for {
          _ <- redisClient.hset(k, f, v)
          r <- redisClient.hget(k, f)
        } yield r)
        result.value shouldBe Some(v)
    }
    "hgetall" in forAll(keyFieldStrValueGen) {
      case (k, f, v) =>
        val result = runProgram(for {
          _ <- redisClient.hset(k, f, v)
          r <- redisClient.hgetAll(k)
        } yield r)
        result.value(0) shouldBe f
        result.value(1) shouldBe v
    }
    "hincrBy" in {}
    "hincrByFloat" in {}
    "hkeys" in {}
    "hlen" in {}
    "hmget" in {}
    "hmset" in {}
    "hscan" in {}
    "hset" in {}
    "hsetnx" in forAll(keyFieldStrValueGen) {
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
    "HVALS" in {}
  }

}
