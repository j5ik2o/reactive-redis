package com.github.j5ik2o.reactive.redis.feature

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.AbstractRedisClientSpec
import org.scalacheck.Shrink

class HashesFeatureSpec extends AbstractRedisClientSpec(ActorSystem("HashesFeatureSpec")) {

  implicit val noShrink: Shrink[String] = Shrink.shrinkAny

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
