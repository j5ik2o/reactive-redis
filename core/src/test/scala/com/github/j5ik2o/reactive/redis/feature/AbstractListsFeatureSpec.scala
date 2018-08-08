package com.github.j5ik2o.reactive.redis.feature

import akka.actor.ActorSystem
import cats.data.NonEmptyList
import cats.implicits._
import com.github.j5ik2o.reactive.redis.AbstractRedisClientSpec
import org.scalacheck.Shrink

import scala.concurrent.duration._

abstract class AbstractListsFeatureSpec extends AbstractRedisClientSpec(ActorSystem("ListsFeatureSpec")) {

  implicit val noShrink1: Shrink[String]       = Shrink.shrinkAny
  implicit val noShrink2: Shrink[List[String]] = Shrink.shrinkAny

  "ListsFeature" - {
    "rpush" in forAll(keyStrValuesGen(4)) {
      case (key, values) =>
        val result = runProgram(for {
          size1 <- redisClient.rPush(key, values(0))
          size2 <- redisClient.rPush(key, values(1))
          size3 <- redisClient.rPush(key, NonEmptyList.of(values(2), values(3)))
        } yield (size1, size2, size3))
        result._1.value shouldBe 1
        result._2.value shouldBe 2
        result._3.value shouldBe 4
    }
    "lpush" in forAll(keyStrValuesGen(4)) {
      case (key, values) =>
        val result = runProgram(for {
          r1 <- redisClient.lPush(key, values(0))
          r2 <- redisClient.lPush(key, values(1))
          r3 <- redisClient.lPush(key, NonEmptyList.of(values(2), values(3)))
          //
        } yield (r1, r2, r3))
        result._1.value shouldBe 1
        result._2.value shouldBe 2
        result._3.value shouldBe 4
    }
    "llen" in forAll(keyStrValuesGen(2)) {
      case (key, values) =>
        val result = runProgram(for {
          r1 <- redisClient.lLen(key)
          _  <- redisClient.lPush(key, values(0))
          _  <- redisClient.lPush(key, values(1))
          r2 <- redisClient.lLen(key)
        } yield (r1, r2))
        result._1.value shouldBe 0
        result._2.value shouldBe 2
    }
    "lrange" in {}
    "ltrim" in {}
    "lset" in {}
    "lindex" in {}
    "lrem" in {}
    "lpop" in forAll(keyStrValueGen) {
      case (key, value) =>
        val result = runProgram(for {
          _  <- redisClient.rPush(key, "a")
          _  <- redisClient.rPush(key, "b")
          _  <- redisClient.rPush(key, "c")
          r1 <- redisClient.lPop(key)
          r2 <- redisClient.lRange(key, 0, 1000)
          _  <- redisClient.lPop(key)
          _  <- redisClient.lPop(key)
          r3 <- redisClient.lPop(key)
        } yield (r1, r2, r3))
        result._1.value shouldBe Some("a")
        result._2.value shouldBe Seq("b", "c")
        result._3.value shouldBe None
    }
    "rpop" in {}
    "rpoplpush" in {}
    "blpop" in forAll(keyStrValueGen) {
      case (key, value) =>
        val result = runProgram(for {
          r1 <- redisClient.blPop(1 seconds, key)
          _  <- redisClient.lPush(key, value)
          r2 <- redisClient.blPop(1 seconds, key)
        } yield (r1, r2))
        result._1.value shouldBe Seq.empty
        result._2.value should not be Seq.empty
        result._2.value should have length 2
        result._2.value(0) shouldBe key
        result._2.value(1) shouldBe value
    }
    "brpop" in {}
    "lpushx" in {}
    "rpushx" in {}
    "linsert" in {}
    "brpoplpush" in {
//      Future {
//        Thread.sleep(100)
//        redisClient.lPush("foo", "a")
//      }
//      val result = runProgram(for {
//        r1 <- redisClient.brPopLPush("foo", "bar", Duration.Inf)
//      } yield r1)
    }
  }

}
