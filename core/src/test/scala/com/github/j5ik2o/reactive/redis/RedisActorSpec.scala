package com.github.j5ik2o.reactive.redis

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.actor.{ ActorSystem, PoisonPill }
import akka.pattern.ask
import com.github.j5ik2o.reactive.redis.Options.StartAndEnd
import com.github.j5ik2o.reactive.redis.StringsOperations.BitFieldRequest.SingedBitType
import com.github.j5ik2o.reactive.redis.StringsOperations._
import com.github.j5ik2o.reactive.redis.TransactionOperations._

import scala.concurrent.duration._

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
      actorRef ! PoisonPill
    }
    // --- BITCOUNT
    it("should be able to BITCOUNT") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue.isInstanceOf[SetSucceeded])
      assert(
        (actorRef ? BitCountRequest(UUID.randomUUID, id)).futureValue
          .asInstanceOf[BitCountSucceeded]
          .value == 3
      )
      actorRef ! PoisonPill
    }
    // --- BITFIELD
    it("should be able to BITFIELD") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      val result =
        (actorRef ? BitFieldRequest(UUID.randomUUID, id, BitFieldRequest.IncrBy(SingedBitType(5), 100, 1))).futureValue
      assert(result.isInstanceOf[BitFieldSucceeded])
      assert(result.asInstanceOf[BitFieldSucceeded].values == List(1))
      actorRef ! PoisonPill
    }
    // --- BITOP
    it("should be able to BITOP") {
      val actorRef = system.actorOf(props)
      val id1      = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id1, "foobar")).futureValue.isInstanceOf[SetSucceeded])
      val id2 = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id2, "abcdef")).futureValue.isInstanceOf[SetSucceeded])
      val id3 = idGenerator.incrementAndGet().toString
      val result =
        (actorRef ? BitOpRequest(UUID.randomUUID, BitOpRequest.Operand.AND, id3, id1, id2)).futureValue
      assert(result.isInstanceOf[BitOpSucceeded])
      assert(result.asInstanceOf[BitOpSucceeded].value == 6)
      assert(
        (actorRef ? GetRequest(UUID.randomUUID, id3)).futureValue.asInstanceOf[GetSucceeded].value.contains("`bc`ab")
      )
      actorRef ! PoisonPill
    }
    // --- BITPOS
    it("should be able to BITPOS") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, """\xff\xf0\x00""")).futureValue.isInstanceOf[SetSucceeded])
      assert((actorRef ? BitPosRequest(UUID.randomUUID, id, 0)).futureValue.asInstanceOf[BitPosSucceeded].value == 12)
      actorRef ! PoisonPill
    }
    // --- DECR
    it("should be able to DECR") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, 10)).futureValue.isInstanceOf[SetSucceeded])
      assert((actorRef ? DecrRequest(UUID.randomUUID(), id)).futureValue.asInstanceOf[DecrSucceeded].value == 9)
      actorRef ! PoisonPill
    }
    // --- DECRBY
    it("should be able to DECRBY") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, 10)).futureValue.isInstanceOf[SetSucceeded])
      assert((actorRef ? DecrByRequest(UUID.randomUUID(), id, 2)).futureValue.asInstanceOf[DecrBySucceeded].value == 8)
      actorRef ! PoisonPill
    }
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
      actorRef ! PoisonPill
    }
    // --- GETBIT
    it("should be able to GETBIT") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert(
        (actorRef ? GetBitRequest(UUID.randomUUID, id, 1)).futureValue
          .asInstanceOf[GetBitSucceeded]
          .value == 0
      )
      actorRef ! PoisonPill
    }
    // --- GETRANGE
    it("should be able to GETRANGE") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "This is a string")).futureValue.isInstanceOf[SetSucceeded])
      assert(
        (actorRef ? GetRangeRequest(UUID.randomUUID, id, StartAndEnd(0, 3))).futureValue
          .asInstanceOf[GetRangeSucceeded]
          .value
          .contains("This")
      )
      actorRef ! PoisonPill
    }
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
      actorRef ! PoisonPill
    }
    // --- INCR
    it("should be able to INCR") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, 1)).futureValue.isInstanceOf[SetSucceeded])
      assert(
        (actorRef ? IncrRequest(UUID.randomUUID, id)).futureValue
          .asInstanceOf[IncrSucceeded]
          .value == 2
      )
      actorRef ! PoisonPill
    }
    // --- INCRBY
    it("should be able to INCRBY") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, 1)).futureValue.isInstanceOf[SetSucceeded])
      assert(
        (actorRef ? IncrByRequest(UUID.randomUUID, id, 2)).futureValue
          .asInstanceOf[IncrBySucceeded]
          .value == 3
      )
      actorRef ! PoisonPill
    }
    // --- INCRBYFLOAT
    it("should be able to INCRBYFLOAT") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, 10.50D)).futureValue.isInstanceOf[SetSucceeded])
      assert(
        (actorRef ? IncrByFloatRequest(UUID.randomUUID, id, 0.1)).futureValue
          .asInstanceOf[IncrByFloatSucceeded]
          .value == 10.6D
      )
      actorRef ! PoisonPill
    }
    // --- MGET
    it("should be able to MGET") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue.isInstanceOf[SetSucceeded])
      assert(
        (actorRef ? MGetRequest(UUID.randomUUID, Seq(id, "b"))).futureValue
          .asInstanceOf[MGetSucceeded]
          .values == Seq(Some("a"), None)
      )
      actorRef ! PoisonPill
    }

    // --- MSET
    it("should be able to MSET") {
      val actorRef = system.actorOf(props)
      val id1      = idGenerator.incrementAndGet().toString
      val id2      = idGenerator.incrementAndGet().toString
      val id3      = idGenerator.incrementAndGet().toString
      assert(
        (actorRef ? MSetRequest(UUID.randomUUID, Map(id1 -> "1", id2 -> "2", id3 -> "3"))).futureValue
          .isInstanceOf[MSetSucceeded]
      )
      assert((actorRef ? GetRequest(UUID.randomUUID, id1)).futureValue.asInstanceOf[GetSucceeded].value.contains("1"))
      assert((actorRef ? GetRequest(UUID.randomUUID, id2)).futureValue.asInstanceOf[GetSucceeded].value.contains("2"))
      assert((actorRef ? GetRequest(UUID.randomUUID, id3)).futureValue.asInstanceOf[GetSucceeded].value.contains("3"))
      actorRef ! PoisonPill
    }
    // --- MSETNX
    it("should be able to MSETNX") {
      val actorRef = system.actorOf(props)
      val id1      = idGenerator.incrementAndGet().toString
      val id2      = idGenerator.incrementAndGet().toString
      val id3      = idGenerator.incrementAndGet().toString
      assert(
        (actorRef ? MSetNxRequest(UUID.randomUUID, Map(id1 -> "1", id2 -> "2", id3 -> "3"))).futureValue
          .asInstanceOf[MSetNxSucceeded]
          .isSet
      )
      assert(
        !(actorRef ? MSetNxRequest(UUID.randomUUID, Map(id1 -> "4", id2 -> "5", id3 -> "6"))).futureValue
          .asInstanceOf[MSetNxSucceeded]
          .isSet
      )
      assert(
        (actorRef ? MGetRequest(UUID.randomUUID, Seq(id1, id2, id3))).futureValue
          .asInstanceOf[MGetSucceeded]
          .values == Seq(Some("1"), Some("2"), Some("3"))
      )
      actorRef ! PoisonPill
    }
    // --- PSETEX
    it("should be able to PSETEX") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert(
        (actorRef ? PSetExRequest(UUID.randomUUID, id, 1000 milliseconds, "a")).futureValue
          .isInstanceOf[PSetExSucceeded]
      )
      Thread.sleep(1500)
      assert((actorRef ? GetRequest(UUID.randomUUID, id)).futureValue.asInstanceOf[GetSucceeded].value.isEmpty)
      actorRef ! PoisonPill
    }
    // --- SET
    it("should be able to SET") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue.isInstanceOf[SetSucceeded])
      assert((actorRef ? GetRequest(UUID.randomUUID, id)).futureValue.asInstanceOf[GetSucceeded].value.contains("a"))
      assert((actorRef ? SetRequest(UUID.randomUUID, id, 1)).futureValue.isInstanceOf[SetSucceeded])
      assert((actorRef ? GetRequest(UUID.randomUUID, id)).futureValue.asInstanceOf[GetSucceeded].value.contains("1"))
      actorRef ! PoisonPill
    }
    // --- SETBIT
    it("should be able to SETBIT") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert(
        (actorRef ? SetBitRequest(UUID.randomUUID, id, 7, 1)).futureValue.asInstanceOf[SetBitSucceeded].value == 0
      )
      assert(
        (actorRef ? SetBitRequest(UUID.randomUUID, id, 7, 0)).futureValue.asInstanceOf[SetBitSucceeded].value == 1
      )
      assert(
        (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue.asInstanceOf[GetSucceeded].value.contains("\u0000")
      )
      actorRef ! PoisonPill
    }
    // --- SETEX
    it("should be able to SETEX") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetExRequest(UUID.randomUUID, id, 1 seconds, "a")).futureValue.isInstanceOf[SetExSucceeded])
      Thread.sleep(1500)
      assert((actorRef ? GetRequest(UUID.randomUUID, id)).futureValue.asInstanceOf[GetSucceeded].value.isEmpty)
      actorRef ! PoisonPill
    }
    // --- SETNX
    it("should be able to SETNX") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetNxRequest(UUID.randomUUID, id, "a")).futureValue.asInstanceOf[SetNxSucceeded].isSet)
      assert(
        !(actorRef ? SetNxRequest(UUID.randomUUID, id, "b")).futureValue.asInstanceOf[SetNxSucceeded].isSet
      )
      actorRef ! PoisonPill
    }
    // --- SETRANGE
    it("should be able to SETRANGE") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "Hello World")).futureValue.isInstanceOf[SetSucceeded])
      assert((actorRef ? SetRangeRequest(UUID.randomUUID, id, 6, "Redis")).futureValue.isInstanceOf[SetRangeSucceeded])
      assert(
        (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue
          .asInstanceOf[GetSucceeded]
          .value
          .contains("Hello Redis")
      )
      actorRef ! PoisonPill
    }
    // --- STRLEN
    it("should be able to STRLEN") {
      val actorRef = system.actorOf(props)
      val id       = idGenerator.incrementAndGet().toString
      assert((actorRef ? SetRequest(UUID.randomUUID, id, "abc")).futureValue.isInstanceOf[SetSucceeded])
      assert((actorRef ? StrLenRequest(UUID.randomUUID(), id)).futureValue.asInstanceOf[StrLenSucceeded].length == 3)
      actorRef ! PoisonPill
    }
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
      actorRef ! PoisonPill
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
      actorRef ! PoisonPill
    }
  }
}
