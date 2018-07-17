package com.github.j5ik2o.reactive.redis.feature

import akka.actor.ActorSystem
import cats.data.NonEmptyList
import cats.implicits._
import com.github.j5ik2o.reactive.redis.AbstractRedisClientSpec
import org.scalacheck.Shrink

class ListsFeatureSpec extends AbstractRedisClientSpec(ActorSystem("ListsFeatureSpec")) {
  implicit val noShrink: Shrink[String] = Shrink.shrinkAny
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
