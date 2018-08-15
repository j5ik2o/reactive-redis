package com.github.j5ik2o.reactive.redis.feature

import java.time.ZonedDateTime

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis._
import org.scalacheck.Shrink
import cats.implicits._

import scala.concurrent.duration._

abstract class AbstractKeysFeatureSpec extends AbstractRedisClientSpec(ActorSystem("KeysFeatureSpec")) {

  implicit val noShrink: Shrink[String] = Shrink.shrinkAny

  "KeysFeature" - {
    "del" in forAll(keyStrValueGen) {
      case (k, v) =>
        val result1 = runProgram(for {
          _ <- redisClient.set(k, v)
          r <- redisClient.del(k)
        } yield r)
        result1.value shouldBe 1
    }
    "dump" in {}
    "exists" in forAll(keyStrValueGen) {
      case (k, v) =>
        val result1 = runProgram(for {
          _ <- redisClient.set(k, v)
          r <- redisClient.exists(k)
        } yield r)
        result1.value shouldBe true
    }
    "expire" in forAll(keyStrValueGen) {
      case (k, v) =>
        val result1 = runProgram(for {
          _  <- redisClient.set(k, v)
          r1 <- redisClient.expire(k, 1 seconds)
          _  <- ReaderTTask.pure(Thread.sleep((3000 * timeFactor).toInt))
          r2 <- redisClient.exists(k)
        } yield (r1, r2))
        result1._1.value shouldBe true
        result1._2.value shouldBe false
    }
    "expireAt" in forAll(keyStrValueGen) {
      case (k, v) =>
        val result1 = runProgram(for {
          _  <- redisClient.set(k, v)
          r1 <- redisClient.expireAt(k, ZonedDateTime.now.plusSeconds(1))
          _  <- ReaderTTask.pure(Thread.sleep((3000 * timeFactor).toInt))
          r2 <- redisClient.exists(k)
        } yield (r1, r2))
        result1._1.value shouldBe true
        result1._2.value shouldBe false
    }
    "keys" in forAll(keyStrValueGen) {
      case (k, v) =>
        val result1 = runProgram(for {
          _ <- redisClient.set(k, v)
          r <- redisClient.keys(k)
        } yield r)
        result1.value shouldBe Seq(k)
    }
    "migrate" in {}
    "move" in {}
    "persist" in {}
    "pExpire" in forAll(keyStrValueGen) {
      case (k, v) =>
        val result1 = runProgram(for {
          _  <- redisClient.set(k, v)
          r1 <- redisClient.pExpire(k, 1 seconds)
          _  <- ReaderTTask.pure(Thread.sleep((3000 * timeFactor).toInt))
          r2 <- redisClient.exists(k)
        } yield (r1, r2))
        result1._1.value shouldBe true
        result1._2.value shouldBe false
    }
    "pExpireAt" in forAll(keyStrValueGen) {
      case (k, v) =>
        val result1 = runProgram(for {
          _  <- redisClient.set(k, v)
          r1 <- redisClient.pExpireAt(k, ZonedDateTime.now.plusSeconds(1))
          _  <- ReaderTTask.pure(Thread.sleep((3000 * timeFactor).toInt))
          r2 <- redisClient.exists(k)
        } yield (r1, r2))
        result1._1.value shouldBe true
        result1._2.value shouldBe false
    }
    "pttl" in forAll(keyStrValueGen) {
      case (k, v) =>
        val result1 = runProgram(for {
          _ <- redisClient.set(k, v)
          _ <- redisClient.expire(k, 1 seconds)
          r <- redisClient.pTtl(k)
        } yield r)
        result1.value.toMillis <= 1000 shouldBe true
    }
    "randomkey" in forAll(keyStrValueGen) {
      case (k, v) =>
        val result1 = runProgram(for {
          key <- redisClient.randomKey()
        } yield key)
        result1.value should not be empty
    }
    "rename" in forAll(keyStrValueGen) {
      case (k, v) =>
        val result1 = runProgram(for {
          _ <- redisClient.set(k, v)
          k2 = s"$k-2"
          _ <- redisClient.rename(k, k2)
          r <- redisClient.get(k2)
        } yield r)
        result1.value shouldBe Some(v)
    }
    "renamenx" in forAll(keyStrValueGen) {
      case (k, v) =>
        val result1 = runProgram(for {
          _ <- redisClient.set(k, v)
          k2 = s"$k-2"
          r1 <- redisClient.renameNx(k, k2)
          r2 <- redisClient.get(k2)
          _  <- redisClient.set(k, v)
          r3 <- redisClient.renameNx(k, k2)
        } yield (r1, r2, r3))
        result1._1.value shouldBe true
        result1._2.value shouldBe Some(v)
        result1._3.value shouldBe false
    }
    "ttl" in forAll(keyStrValueGen) {
      case (k, v) =>
        val result1 = runProgram(for {
          _ <- redisClient.set(k, v)
          _ <- redisClient.expire(k, 1 seconds)
          r <- redisClient.ttl(k)
        } yield r)
        result1.value.toMillis <= 1000 shouldBe true
    }
  }

}
