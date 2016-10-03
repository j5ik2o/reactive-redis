package com.github.j5ik2o.reactive.redis

import java.util.UUID

import akka.actor.{ ActorSystem, Props }
import akka.pattern.ask
import akka.stream.actor.ActorPublisher
import com.github.j5ik2o.reactive.redis.StringOperations._
import com.github.j5ik2o.reactive.redis.TransactionOperations.{ ExecRequest, MultiRequest }
import org.scalatest.BeforeAndAfter

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration.Duration


class RedisActorSpec
  extends ActorSpec(ActorSystem("RedisActorSpec")) with ServerBootable
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

  val actorRef = system.actorOf(RedisActor.props("127.0.0.1", testServer.address.get.getPort))

  describe("StringOperations") {
    it("should be set value") {
      assert((actorRef ? SetRequest(UUID.randomUUID, "1", "a")).futureValue.isInstanceOf[SetSucceeded])
    }
    it("should be got value") {
      assert((actorRef ? SetRequest(UUID.randomUUID, "2", "a")).futureValue.isInstanceOf[SetSucceeded])
      assert((actorRef ? GetRequest(UUID.randomUUID, "2")).futureValue.asInstanceOf[GetSucceeded].value.contains("a"))
    }
    it("should be got and set value") {
      assert((actorRef ? SetRequest(UUID.randomUUID, "2", "a")).futureValue.isInstanceOf[SetSucceeded])
      assert((actorRef ? GetSetRequest(UUID.randomUUID, "2", "b")).futureValue.asInstanceOf[GetSetSucceeded].value.contains("a"))
      assert((actorRef ? GetRequest(UUID.randomUUID, "2")).futureValue.asInstanceOf[GetSucceeded].value.contains("b"))
    }
  }
  describe("TransactionOperations") {
    it("should be able to transactional") {
      actorRef ! MultiRequest(UUID.randomUUID())
      actorRef ! SetRequest(UUID.randomUUID, "2", "a")
      actorRef ! GetRequest(UUID.randomUUID, "2")
      Await.result(actorRef ? ExecRequest(UUID.randomUUID()), Duration.Inf)
    }
  }
}
