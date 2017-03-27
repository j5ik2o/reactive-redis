package com.github.j5ik2o.reactive.redis

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.StringOperations.{ SetRequest, SetSucceeded }
import org.scalatest.BeforeAndAfter
import akka.pattern.ask

class RedisSupervisorSpec
    extends ActorSpec(ActorSystem("RedisSupervisorSpec"))
    with BeforeAndAfter {

  val idGenerator = new AtomicLong()

  val testServer: TestServer = new TestServer(portOpt = Some(6200))

  override protected def beforeAll(): Unit = {
    testServer.start()
  }

  override protected def afterAll(): Unit = {
    testServer.stop()
  }

  describe("RedisSupervisor") {
    it("should be able to restart the redis-actor") {
      val actorRef = system.actorOf(
        RedisSupervisor.props(
          RedisActor.props(UUID.randomUUID, "127.0.0.1", testServer.getPort),
          UUID.randomUUID
        )
      )

      println("port = " + testServer.getPort)

      testServer.stop()

      val id1 = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id1, "a")).futureValue.isInstanceOf[SetSucceeded])

      testServer.start()

      println("port = " + testServer.getPort)

      val id2 = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id2, "a")).futureValue.isInstanceOf[SetSucceeded])

    }
  }

}
