package com.github.j5ik2o.reactive.redis

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.actor.{ ActorSystem, Props }
import akka.pattern.ask
import akka.stream.actor.ActorPublisher
import com.github.j5ik2o.reactive.redis.StringsOperations._
import com.github.j5ik2o.reactive.redis.TransactionOperations._
import org.scalatest.BeforeAndAfter

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class RedisActorSpec extends ActorSpec(ActorSystem("RedisActorSpec")) with RedisServerSupport {

  val idGenerator = new AtomicLong()

  def props = RedisActor.props(UUID.randomUUID, "127.0.0.1", testServer.address.get.getPort)

  describe("StringOperations") {
    // APPEND
    it("should be able to APPEND") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert(
        (actorRef ? AppendRequest(UUID.randomUUID, id, "1")).futureValue
          .asInstanceOf[AppendSucceeded]
          .value == 1
      )
      assert(
        (actorRef ? AppendRequest(UUID.randomUUID, id, "2")).futureValue
          .asInstanceOf[AppendSucceeded]
          .value == 2
      )
      assert(
        (actorRef ? AppendRequest(UUID.randomUUID, id, "3")).futureValue
          .asInstanceOf[AppendSucceeded]
          .value == 3
      )
      assert(
        (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue
          .asInstanceOf[GetSucceeded]
          .value
          .contains("123")
      )
    }
    it("should be able to BITCOUNT") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue.isInstanceOf[SetSucceeded])
      assert(
        (actorRef ? BitCountRequest(UUID.randomUUID, id)).futureValue
          .asInstanceOf[BitCountSucceeded]
          .value == 3
      )
    }
    // --- BITFIELD
    // --- BITOP
    // --- BITPOS
    // --- DECR
    // --- DECRBY
    // --- GET
    it("should be able to GET") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue.isInstanceOf[SetSucceeded])
      assert(
        (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue
          .asInstanceOf[GetSucceeded]
          .value
          .contains("a")
      )
    }
    // --- GETBIT
    // --- GETRANGE
    // ---  GETSET
    it("should be able to GETSET") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue.isInstanceOf[SetSucceeded])
      assert(
        (actorRef ? GetSetRequest(UUID.randomUUID, id, "b")).futureValue
          .asInstanceOf[GetSetSucceeded]
          .value
          .contains("a")
      )
      assert(
        (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue
          .asInstanceOf[GetSucceeded]
          .value
          .contains("b")
      )
    }
    // --- INCR
    it("should be able to INCR") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "1")).futureValue.isInstanceOf[SetSucceeded])
      assert(
        (actorRef ? IncrRequest(UUID.randomUUID, id)).futureValue
          .asInstanceOf[IncrSucceeded]
          .value == 2
      )
      assert(
        (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue
          .asInstanceOf[GetSucceeded]
          .value
          .contains("2")
      )
    }
    // --- INCRBYFLOAT
    // --- MGET
    // --- MSET
    // --- MSETNX
    // --- PSETEX
    // --- SET
    it("should be able to SET") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue.isInstanceOf[SetSucceeded])
    }
    // --- SETBIT
    // --- SETEX
    // --- SETNX
    // --- SETRANGE
    // --- STRLEN
  }
  describe("TransactionOperations") {
    it("should be able to execute transactional commands") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "c")).futureValue.isInstanceOf[SetSucceeded])
      assert((actorRef ? MultiRequest(UUID.randomUUID())).futureValue.isInstanceOf[MultiSucceeded])
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue.isInstanceOf[SetSuspended])
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "b")).futureValue.isInstanceOf[SetSuspended])
      assert((actorRef ? GetRequest(UUID.randomUUID, id)).futureValue.isInstanceOf[GetSuspended])
      val execResult =
        (actorRef ? ExecRequest(UUID.randomUUID())).futureValue.asInstanceOf[ExecSucceeded]
      assert(execResult.responses(0).isInstanceOf[SetSucceeded])
      assert(execResult.responses(1).isInstanceOf[SetSucceeded])
      assert(execResult.responses(2).asInstanceOf[GetSucceeded].value.contains("b"))
      assert(
        (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue
          .asInstanceOf[GetSucceeded]
          .value
          .contains("b")
      )
    }
    it("should be able to discard transactional commands") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "c")).futureValue.isInstanceOf[SetSucceeded])
      assert((actorRef ? MultiRequest(UUID.randomUUID())).futureValue.isInstanceOf[MultiSucceeded])
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue.isInstanceOf[SetSuspended])
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "b")).futureValue.isInstanceOf[SetSuspended])
      assert((actorRef ? GetRequest(UUID.randomUUID, id)).futureValue.isInstanceOf[GetSuspended])
      assert((actorRef ? DiscardRequest(UUID.randomUUID())).futureValue.isInstanceOf[DiscardSucceeded])
      assert(
        (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue
          .asInstanceOf[GetSucceeded]
          .value
          .contains("c")
      )
    }
  }
}
