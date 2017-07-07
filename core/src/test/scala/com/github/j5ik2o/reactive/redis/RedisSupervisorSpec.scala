package com.github.j5ik2o.reactive.redis

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.actor.{ ActorSystem, PoisonPill }
import akka.pattern.ask
import com.github.j5ik2o.reactive.redis.StringsOperations.{ SetRequest, SetSucceeded }

class RedisSupervisorSpec extends ActorSpec(ActorSystem("RedisSupervisorSpec")) with RedisServerSupport {

  val idGenerator = new AtomicLong()

  describe("RedisSupervisor") {
    it("should be able to restart the redis-actor") {
      val actorRef = system.actorOf(
        RedisSupervisor.props(
          RedisActor.props(UUID.randomUUID, "127.0.0.1", testServer.getPort),
          UUID.randomUUID
        )
      )

      val id1          = idGenerator.incrementAndGet().toString
      val setResponse1 = (actorRef ? SetRequest(UUID.randomUUID, id1, "a")).futureValue
      assert(setResponse1.isInstanceOf[SetSucceeded])

      val id2          = idGenerator.incrementAndGet().toString
      val setResponse2 = (actorRef ? SetRequest(UUID.randomUUID, id2, "a")).futureValue
      assert(setResponse2.isInstanceOf[SetSucceeded])

      actorRef ! PoisonPill
    }
  }

}
