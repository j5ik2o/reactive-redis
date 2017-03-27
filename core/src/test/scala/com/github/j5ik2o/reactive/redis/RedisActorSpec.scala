package com.github.j5ik2o.reactive.redis

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.actor.{ ActorSystem, Props }
import akka.pattern.ask
import akka.stream.actor.ActorPublisher
import com.github.j5ik2o.reactive.redis.StringOperations._
import com.github.j5ik2o.reactive.redis.TransactionOperations._
import org.scalatest.BeforeAndAfter

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class RedisActorSpec
    extends ActorSpec(ActorSystem("RedisActorSpec"))
    with ServerBootable
    with BeforeAndAfter {

  //  override protected def beforeAll(): Unit = {
  //    super.beforeAll()
  //    val client = system.actorOf(StringClient.props(new InetSocketAddress("127.0.0.1", testServer.address.get.getPort)))
  //    clientRef.set(client)
  //  }
  //
  //  override protected def afterAll(): Unit = {
  //    //Await.result(client ? QuitRequest, 10 seconds)
  //    system.terminate()
  //    super.afterAll()
  //  }

  val idGenerator = new AtomicLong()

  describe("StringOperations") {
    it("should be set value") {
      val actorRef = system.actorOf(RedisActor.props(UUID.randomUUID, "127.0.0.1", testServer.address.get.getPort))
      val id = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue.isInstanceOf[SetSucceeded])
    }
    it("should be got value") {
      val actorRef = system.actorOf(RedisActor.props(UUID.randomUUID, "127.0.0.1", testServer.address.get.getPort))
      val id = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue.isInstanceOf[SetSucceeded])
      assert((actorRef ? GetRequest(UUID.randomUUID, id)).futureValue.asInstanceOf[GetSucceeded].value.contains("a"))
    }
    it("should be got and set value") {
      val actorRef = system.actorOf(RedisActor.props(UUID.randomUUID, "127.0.0.1", testServer.address.get.getPort))
      val id = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue.isInstanceOf[SetSucceeded])
      assert((actorRef ? GetSetRequest(UUID.randomUUID, id, "b")).futureValue.asInstanceOf[GetSetSucceeded].value.contains("a"))
      assert((actorRef ? GetRequest(UUID.randomUUID, id)).futureValue.asInstanceOf[GetSucceeded].value.contains("b"))
    }
  }
  describe("TransactionOperations") {
    it("should be able to execute transactional commands") {
      val actorRef = system.actorOf(RedisActor.props(UUID.randomUUID, "127.0.0.1", testServer.address.get.getPort))
      val id = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "c")).futureValue.isInstanceOf[SetSucceeded])
      assert((actorRef ? MultiRequest(UUID.randomUUID())).futureValue.isInstanceOf[MultiSucceeded])
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue.isInstanceOf[SetSuspended])
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "b")).futureValue.isInstanceOf[SetSuspended])
      assert((actorRef ? GetRequest(UUID.randomUUID, id)).futureValue.isInstanceOf[GetSuspended])
      val execResult = (actorRef ? ExecRequest(UUID.randomUUID())).futureValue.asInstanceOf[ExecSucceeded]
      assert(execResult.responses(0).isInstanceOf[SetSucceeded])
      assert(execResult.responses(1).isInstanceOf[SetSucceeded])
      assert(execResult.responses(2).asInstanceOf[GetSucceeded].value.contains("b"))
      assert((actorRef ? GetRequest(UUID.randomUUID, id)).futureValue.asInstanceOf[GetSucceeded].value.contains("b"))
    }
    it("should be able to discard transactional commands") {
      val actorRef = system.actorOf(RedisActor.props(UUID.randomUUID, "127.0.0.1", testServer.address.get.getPort))
      val id = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "c")).futureValue.isInstanceOf[SetSucceeded])
      assert((actorRef ? MultiRequest(UUID.randomUUID())).futureValue.isInstanceOf[MultiSucceeded])
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue.isInstanceOf[SetSuspended])
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "b")).futureValue.isInstanceOf[SetSuspended])
      assert((actorRef ? GetRequest(UUID.randomUUID, id)).futureValue.isInstanceOf[GetSuspended])
      assert((actorRef ? DiscardRequest(UUID.randomUUID())).futureValue.isInstanceOf[DiscardSucceeded])
      assert((actorRef ? GetRequest(UUID.randomUUID, id)).futureValue.asInstanceOf[GetSucceeded].value.contains("c"))
    }
  }
}
