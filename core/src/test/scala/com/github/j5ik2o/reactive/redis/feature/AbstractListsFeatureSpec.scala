package com.github.j5ik2o.reactive.redis.feature

import akka.actor.ActorSystem
import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.AbstractRedisClientSpec
import org.scalacheck.Shrink

import scala.concurrent.duration._
import cats.implicits._

abstract class AbstractListsFeatureSpec extends AbstractRedisClientSpec(ActorSystem("ListsFeatureSpec")) {

  implicit val noShrink1: Shrink[String]       = Shrink.shrinkAny
  implicit val noShrink2: Shrink[List[String]] = Shrink.shrinkAny

  "ListsFeature" - {
    "rpush" in {}
    "lpush" in forAll(keyStrValuesGen(4)) {
      case (key, values) =>
        val result = runProgram(for {
          r1 <- redisClient.lpush(key, values(0))
          r2 <- redisClient.lpush(key, values(1))
          r3 <- redisClient.lpush(key, NonEmptyList.of("bar", "foo"))
          //
        } yield (r1, r2, r3))
        result._1.value shouldBe 1
        result._2.value shouldBe 2
        result._3.value shouldBe 4
    }
    "llen" in {}
    "lrange" in {}
    "ltrim" in {}
    "lset" in {}
    "lindex" in {}
    "lrem" in {}
    "lpop" in forAll(keyStrValueGen) {
      case (key, value) =>
        val result = runProgram(for {
          _  <- redisClient.rpush(key, "a")
          _  <- redisClient.rpush(key, "b")
          _  <- redisClient.rpush(key, "c")
          r1 <- redisClient.lpop(key)
          r2 <- redisClient.lrange(key, 0, 1000)
          _  <- redisClient.lpop(key)
          _  <- redisClient.lpop(key)
          r3 <- redisClient.lpop(key)
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
          r1 <- redisClient.blpop(1 seconds, key)
          _  <- redisClient.lpush(key, value)
          r2 <- redisClient.blpop(1 seconds, key)
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
    "brpoplpush" in {}
  }

}
