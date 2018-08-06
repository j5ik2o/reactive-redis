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
