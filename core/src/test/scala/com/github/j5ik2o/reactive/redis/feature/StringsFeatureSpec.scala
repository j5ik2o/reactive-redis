package com.github.j5ik2o.reactive.redis.feature

import akka.actor.ActorSystem
import cats.implicits._
import com.github.j5ik2o.reactive.redis.AbstractRedisClientSpec
import org.scalacheck.Shrink

class StringsFeatureSpec extends AbstractRedisClientSpec(ActorSystem("StringsFeatureSpec")) {
  implicit val noShrink: Shrink[String] = Shrink.shrinkAny

  "StringsFeature" - {
    "set & get" in forAll(keyValueGen) {
      case (k, v) =>
        val result = runProgram(for {
          _      <- redisClient.set(k, v)
          result <- redisClient.get(k)
        } yield result)

        result.value should not be empty
    }
  }

}
