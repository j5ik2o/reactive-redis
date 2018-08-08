package com.github.j5ik2o.reactive.redis.feature

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.AbstractRedisClientSpec

abstract class AbstractSetsFeatureSpec extends AbstractRedisClientSpec(ActorSystem("SetsFeatureSpec")) {
  "SetsFeature" - {
    "sadd" in {
      val result = runProgram(for {
        status1 <- redisClient.sAdd("foo", "a")
        status2 <- redisClient.sAdd("foo", "a")
      } yield (status1, status2))
      result._1.value shouldBe 1
      result._2.value shouldBe 0
    }
    "smembers" in {}
  }
}
