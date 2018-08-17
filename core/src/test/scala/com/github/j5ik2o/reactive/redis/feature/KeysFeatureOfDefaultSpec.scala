package com.github.j5ik2o.reactive.redis.feature

import akka.routing.DefaultResizer
import cats.data.NonEmptyList
import cats.implicits._
import com.github.j5ik2o.reactive.redis.{ PeerConfig, RedisConnection, RedisConnectionPool }
import monix.eval.Task

class KeysFeatureOfDefaultSpec extends AbstractKeysFeatureSpec {

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
  "KeysFeatureOfDefaultSpec" - {
    "unlink" in forAll(keyStrValueGen) {
      case (k, v) =>
        val result1 = runProgram(for {
          _ <- redisClient.set(k, v)
          r <- redisClient.unlink(k)
        } yield r)
        result1.value shouldBe 1
    }
    "touch" in forAll(keyStrValueGen) {
      case (k, v) =>
        val result1 = runProgram(for {
          _ <- redisClient.set(k, v)
          r <- redisClient.touch(k)
        } yield r)
        result1.value shouldBe 1
    }
  }
}
