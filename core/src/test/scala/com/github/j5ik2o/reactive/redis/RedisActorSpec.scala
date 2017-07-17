package com.github.j5ik2o.reactive.redis

import java.util.UUID

import akka.actor.{ ActorSystem, PoisonPill }
import akka.pattern.ask
import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.Options.StartAndEnd
import com.github.j5ik2o.reactive.redis.StringsOperations.BitFieldRequest.SingedBitType
import com.github.j5ik2o.reactive.redis.StringsOperations._
import com.github.j5ik2o.reactive.redis.KeysOperations._
import com.github.j5ik2o.reactive.redis.TransactionOperations._

import scala.concurrent.duration._

class RedisActorSpec extends ActorSpec(ActorSystem("RedisActorSpec")) with RedisServerSupport {

  def props = RedisActor.props(UUID.randomUUID, "127.0.0.1", testServer.address.get.getPort)

  describe("StringOperations") {
    // APPEND
    it("should be able to APPEND") {
      val actorRef = system.actorOf(props)
      val id       = UUID.randomUUID().toString
      val appendResponse1 = (actorRef ? AppendRequest(UUID.randomUUID, id, "1")).futureValue
        .asInstanceOf[AppendSucceeded]
      val appendResponse2 = (actorRef ? AppendRequest(UUID.randomUUID, id, "2")).futureValue
        .asInstanceOf[AppendSucceeded]
      val appendResponse3 = (actorRef ? AppendRequest(UUID.randomUUID, id, "3")).futureValue
        .asInstanceOf[AppendSucceeded]
      val getResponse = (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue
        .asInstanceOf[GetSucceeded]
      assert(appendResponse1.value == 1)
      assert(appendResponse2.value == 2)
      assert(appendResponse3.value == 3)
      assert(getResponse.value.contains("123"))
      actorRef ! PoisonPill
    }
    // --- BITCOUNT
    it("should be able to BITCOUNT") {
      val actorRef    = system.actorOf(props)
      val id          = UUID.randomUUID().toString
      val setResponse = (actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue
      val bitCountResponse =
        (actorRef ? BitCountRequest(UUID.randomUUID, id)).futureValue.asInstanceOf[BitCountSucceeded]
      assert(setResponse.isInstanceOf[SetSucceeded])
      assert(bitCountResponse.value == 3)
      actorRef ! PoisonPill
    }
    // --- BITFIELD
    it("should be able to BITFIELD") {
      val actorRef = system.actorOf(props)
      val id       = UUID.randomUUID().toString
      val result =
        (actorRef ? BitFieldRequest(UUID.randomUUID, id, BitFieldRequest.IncrBy(SingedBitType(5), 100, 1))).futureValue
          .asInstanceOf[BitFieldSucceeded]
      assert(result.values == List(1))
      actorRef ! PoisonPill
    }
    // --- BITOP
    it("should be able to BITOP") {
      val actorRef     = system.actorOf(props)
      val id1          = UUID.randomUUID().toString
      val id2          = UUID.randomUUID().toString
      val id3          = UUID.randomUUID().toString
      val setResponse1 = (actorRef ? SetRequest(UUID.randomUUID, id1, "foobar")).futureValue
      val setResponse2 = (actorRef ? SetRequest(UUID.randomUUID, id2, "abcdef")).futureValue
      val result =
        (actorRef ? BitOpRequest(UUID.randomUUID, BitOpRequest.Operand.AND, id3, id1, id2)).futureValue
          .asInstanceOf[BitOpSucceeded]
      val getResponse = (actorRef ? GetRequest(UUID.randomUUID, id3)).futureValue.asInstanceOf[GetSucceeded]
      assert(setResponse1.isInstanceOf[SetSucceeded])
      assert(setResponse2.isInstanceOf[SetSucceeded])
      assert(result.value == 6)
      assert(getResponse.value.contains("`bc`ab"))
      actorRef ! PoisonPill
    }
    // --- BITPOS
    it("should be able to BITPOS") {
      val actorRef       = system.actorOf(props)
      val id             = UUID.randomUUID().toString
      val setResponse    = (actorRef ? SetRequest(UUID.randomUUID, id, """\xff\xf0\x00""")).futureValue
      val bitPosResponse = (actorRef ? BitPosRequest(UUID.randomUUID, id, 0)).futureValue.asInstanceOf[BitPosSucceeded]
      assert(setResponse.isInstanceOf[SetSucceeded])
      assert(bitPosResponse.value == 12)
      actorRef ! PoisonPill
    }
    // --- DECR
    it("should be able to DECR") {
      val actorRef     = system.actorOf(props)
      val id           = UUID.randomUUID().toString
      val setResponse  = (actorRef ? SetRequest(UUID.randomUUID, id, 10)).futureValue
      val decrResponse = (actorRef ? DecrRequest(UUID.randomUUID(), id)).futureValue.asInstanceOf[DecrSucceeded]
      assert(setResponse.isInstanceOf[SetSucceeded])
      assert(decrResponse.value == 9)
      actorRef ! PoisonPill
    }
    // --- DECRBY
    it("should be able to DECRBY") {
      val actorRef    = system.actorOf(props)
      val id          = UUID.randomUUID().toString
      val setResponse = (actorRef ? SetRequest(UUID.randomUUID, id, 10)).futureValue
      val decrByResponse =
        (actorRef ? DecrByRequest(UUID.randomUUID(), id, 2)).futureValue.asInstanceOf[DecrBySucceeded]
      assert(setResponse.isInstanceOf[SetSucceeded])
      assert(decrByResponse.value == 8)
      actorRef ! PoisonPill
    }
    // --- GET
    it("should be able to GET") {
      val actorRef    = system.actorOf(props)
      val id          = UUID.randomUUID().toString
      val setResponse = (actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue
      val getResponse = (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue
        .asInstanceOf[GetSucceeded]
      assert(setResponse.isInstanceOf[SetSucceeded])
      assert(getResponse.value.contains("a"))
      actorRef ! PoisonPill
    }
    // --- GETBIT
    it("should be able to GETBIT") {
      val actorRef = system.actorOf(props)
      val id       = UUID.randomUUID().toString
      val getBitResponse = (actorRef ? GetBitRequest(UUID.randomUUID, id, 1)).futureValue
        .asInstanceOf[GetBitSucceeded]
      assert(getBitResponse.value == 0)
      actorRef ! PoisonPill
    }
    // --- GETRANGE
    it("should be able to GETRANGE") {
      val actorRef    = system.actorOf(props)
      val id          = UUID.randomUUID().toString
      val setResponse = (actorRef ? SetRequest(UUID.randomUUID, id, "This is a string")).futureValue
      val getRangeResponse = (actorRef ? GetRangeRequest(UUID.randomUUID, id, StartAndEnd(0, 3))).futureValue
        .asInstanceOf[GetRangeSucceeded]
      assert(setResponse.isInstanceOf[SetSucceeded])
      assert(getRangeResponse.value.contains("This"))
      actorRef ! PoisonPill
    }
    // ---  GETSET
    it("should be able to GETSET") {
      val actorRef    = system.actorOf(props)
      val id          = UUID.randomUUID().toString
      val setResponse = (actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue
      val getSetResponse = (actorRef ? GetSetRequest(UUID.randomUUID, id, "b")).futureValue
        .asInstanceOf[GetSetSucceeded]
      val getResponse = (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue
        .asInstanceOf[GetSucceeded]
      assert(setResponse.isInstanceOf[SetSucceeded])
      assert(getSetResponse.value.contains("a"))
      assert(getResponse.value.contains("b"))
      actorRef ! PoisonPill
    }
    // --- INCR
    it("should be able to INCR") {
      val actorRef    = system.actorOf(props)
      val id          = UUID.randomUUID().toString
      val setResponse = (actorRef ? SetRequest(UUID.randomUUID, id, 1)).futureValue
      val incrResponse = (actorRef ? IncrRequest(UUID.randomUUID, id)).futureValue
        .asInstanceOf[IncrSucceeded]
      assert(setResponse.isInstanceOf[SetSucceeded])
      assert(incrResponse.value == 2)
      actorRef ! PoisonPill
    }
    // --- INCRBY
    it("should be able to INCRBY") {
      val actorRef    = system.actorOf(props)
      val id          = UUID.randomUUID().toString
      val setResponse = (actorRef ? SetRequest(UUID.randomUUID, id, 1)).futureValue
      val incrByResponse = (actorRef ? IncrByRequest(UUID.randomUUID, id, 2)).futureValue
        .asInstanceOf[IncrBySucceeded]
      assert(setResponse.isInstanceOf[SetSucceeded])
      assert(incrByResponse.value == 3)
      actorRef ! PoisonPill
    }
    // --- INCRBYFLOAT
    it("should be able to INCRBYFLOAT") {
      val actorRef    = system.actorOf(props)
      val id          = UUID.randomUUID().toString
      val setResponse = (actorRef ? SetRequest(UUID.randomUUID, id, 10.50D)).futureValue
      val incrByFloatResponse = (actorRef ? IncrByFloatRequest(UUID.randomUUID, id, 0.1)).futureValue
        .asInstanceOf[IncrByFloatSucceeded]
      assert(setResponse.isInstanceOf[SetSucceeded])
      assert(incrByFloatResponse.value == 10.6D)
      actorRef ! PoisonPill
    }
    // --- MGET
    it("should be able to MGET") {
      val actorRef    = system.actorOf(props)
      val id          = UUID.randomUUID().toString
      val setResponse = (actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue
      val mGetResponse = (actorRef ? MGetRequest(UUID.randomUUID, Seq(id, "b"))).futureValue
        .asInstanceOf[MGetSucceeded]
      assert(setResponse.isInstanceOf[SetSucceeded])
      assert(mGetResponse.values == Seq(Some("a"), None))
      actorRef ! PoisonPill
    }
    // --- MSET
    it("should be able to MSET") {
      val actorRef     = system.actorOf(props)
      val id1          = UUID.randomUUID().toString
      val id2          = UUID.randomUUID().toString
      val id3          = UUID.randomUUID().toString
      val setResponse  = (actorRef ? MSetRequest(UUID.randomUUID, Map(id1 -> "1", id2 -> "2", id3 -> "3"))).futureValue
      val getResponse1 = (actorRef ? GetRequest(UUID.randomUUID, id1)).futureValue.asInstanceOf[GetSucceeded]
      val getResponse2 = (actorRef ? GetRequest(UUID.randomUUID, id2)).futureValue.asInstanceOf[GetSucceeded]
      val getResponse3 = (actorRef ? GetRequest(UUID.randomUUID, id3)).futureValue.asInstanceOf[GetSucceeded]
      assert(setResponse.isInstanceOf[MSetSucceeded])
      assert(getResponse1.value.contains("1"))
      assert(getResponse2.value.contains("2"))
      assert(getResponse3.value.contains("3"))
      actorRef ! PoisonPill
    }
    // --- MSETNX
    it("should be able to MSETNX") {
      val actorRef = system.actorOf(props)
      val id1      = UUID.randomUUID().toString
      val id2      = UUID.randomUUID().toString
      val id3      = UUID.randomUUID().toString
      val mSetNxResponse1 =
        (actorRef ? MSetNxRequest(UUID.randomUUID, Map(id1 -> "1", id2 -> "2", id3 -> "3"))).futureValue
          .asInstanceOf[MSetNxSucceeded]
      val mSetNxResponse2 =
        (actorRef ? MSetNxRequest(UUID.randomUUID, Map(id1 -> "4", id2 -> "5", id3 -> "6"))).futureValue
          .asInstanceOf[MSetNxSucceeded]
      val mGetResponse = (actorRef ? MGetRequest(UUID.randomUUID, Seq(id1, id2, id3))).futureValue
        .asInstanceOf[MGetSucceeded]
      assert(mSetNxResponse1.isSet)
      assert(!mSetNxResponse2.isSet)
      assert(mGetResponse.values == Seq(Some("1"), Some("2"), Some("3")))
      actorRef ! PoisonPill
    }
    // --- PSETEX
    it("should be able to PSETEX") {
      val actorRef       = system.actorOf(props)
      val id             = UUID.randomUUID().toString
      val pSexExResponse = (actorRef ? PSetExRequest(UUID.randomUUID, id, 1000 milliseconds, "a")).futureValue
      assert(pSexExResponse.isInstanceOf[PSetExSucceeded])
      Thread.sleep(1500)
      val getResponse = (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue.asInstanceOf[GetSucceeded]
      assert(getResponse.value.isEmpty)
      actorRef ! PoisonPill
    }
    // --- SET
    it("should be able to SET") {
      val actorRef     = system.actorOf(props)
      val id           = UUID.randomUUID().toString
      val setResponse1 = (actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue
      val getResponse1 = (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue.asInstanceOf[GetSucceeded]
      val setResponse2 = (actorRef ? SetRequest(UUID.randomUUID, id, 1)).futureValue
      val getResponse2 = (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue.asInstanceOf[GetSucceeded]
      assert(setResponse1.isInstanceOf[SetSucceeded])
      assert(getResponse1.value.contains("a"))
      assert(setResponse2.isInstanceOf[SetSucceeded])
      assert(getResponse2.value.contains("1"))
      actorRef ! PoisonPill
    }
    // --- SETBIT
    it("should be able to SETBIT") {
      val actorRef = system.actorOf(props)
      val id       = UUID.randomUUID().toString
      val setBitResponse1 =
        (actorRef ? SetBitRequest(UUID.randomUUID, id, 7, 1)).futureValue.asInstanceOf[SetBitSucceeded]
      val setBitResponse2 =
        (actorRef ? SetBitRequest(UUID.randomUUID, id, 7, 0)).futureValue.asInstanceOf[SetBitSucceeded]
      val getResponse = (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue.asInstanceOf[GetSucceeded]
      assert(setBitResponse1.value == 0)
      assert(setBitResponse2.value == 1)
      assert(getResponse.value.contains("\u0000"))
      actorRef ! PoisonPill
    }
    // --- SETEX
    it("should be able to SETEX") {
      val actorRef      = system.actorOf(props)
      val id            = UUID.randomUUID().toString
      val setExResponse = (actorRef ? SetExRequest(UUID.randomUUID, id, 1 seconds, "a")).futureValue
      assert(setExResponse.isInstanceOf[SetExSucceeded])
      Thread.sleep(1500)
      val getResponse = (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue.asInstanceOf[GetSucceeded]
      assert(getResponse.value.isEmpty)
      actorRef ! PoisonPill
    }
    // --- SETNX
    it("should be able to SETNX") {
      val actorRef       = system.actorOf(props)
      val id             = UUID.randomUUID().toString
      val setNxResponse1 = (actorRef ? SetNxRequest(UUID.randomUUID, id, "a")).futureValue.asInstanceOf[SetNxSucceeded]
      val setNxResponse2 = (actorRef ? SetNxRequest(UUID.randomUUID, id, "b")).futureValue.asInstanceOf[SetNxSucceeded]
      assert(setNxResponse1.isSet)
      assert(!setNxResponse2.isSet)
      actorRef ! PoisonPill
    }
    // --- SETRANGE
    it("should be able to SETRANGE") {
      val actorRef         = system.actorOf(props)
      val id               = UUID.randomUUID().toString
      val setResponse      = (actorRef ? SetRequest(UUID.randomUUID, id, "Hello World")).futureValue
      val setRangeResponse = (actorRef ? SetRangeRequest(UUID.randomUUID, id, 6, "Redis")).futureValue
      val getResponse = (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue
        .asInstanceOf[GetSucceeded]
      assert(setResponse.isInstanceOf[SetSucceeded])
      assert(setRangeResponse.isInstanceOf[SetRangeSucceeded])
      assert(getResponse.value.contains("Hello Redis"))
      actorRef ! PoisonPill
    }
    // --- STRLEN
    it("should be able to STRLEN") {
      val actorRef       = system.actorOf(props)
      val id             = UUID.randomUUID().toString
      val setResponse    = (actorRef ? SetRequest(UUID.randomUUID, id, "abc")).futureValue
      val strLenResponse = (actorRef ? StrLenRequest(UUID.randomUUID(), id)).futureValue.asInstanceOf[StrLenSucceeded]
      assert(setResponse.isInstanceOf[SetSucceeded])
      assert(strLenResponse.length == 3)
      actorRef ! PoisonPill
    }
  }
  describe("TransactionOperations") {
    it("should be able to execute transactional commands") {
      val actorRef      = system.actorOf(props)
      val id            = UUID.randomUUID().toString
      val setResponse1  = (actorRef ? SetRequest(UUID.randomUUID, id, "c")).futureValue
      val multiResponse = (actorRef ? MultiRequest(UUID.randomUUID())).futureValue
      val setResponse2  = (actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue
      val setResponse3  = (actorRef ? SetRequest(UUID.randomUUID, id, "b")).futureValue
      val getResponse1  = (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue
      val execResult =
        (actorRef ? ExecRequest(UUID.randomUUID())).futureValue.asInstanceOf[ExecSucceeded]
      val getResponse2 = (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue
        .asInstanceOf[GetSucceeded]
      assert(setResponse1.isInstanceOf[SetSucceeded])
      assert(multiResponse.isInstanceOf[MultiSucceeded])
      assert(setResponse2.isInstanceOf[SetSuspended])
      assert(setResponse3.isInstanceOf[SetSuspended])
      assert(getResponse1.isInstanceOf[GetSuspended])
      assert(execResult.responses(0).isInstanceOf[SetSucceeded])
      assert(execResult.responses(1).isInstanceOf[SetSucceeded])
      assert(execResult.responses(2).asInstanceOf[GetSucceeded].value.contains("b"))
      assert(getResponse2.value.contains("b"))
      actorRef ! PoisonPill
    }
    it("should be able to discard transactional commands") {
      val actorRef        = system.actorOf(props)
      val id              = UUID.randomUUID().toString
      val setResponse1    = (actorRef ? SetRequest(UUID.randomUUID, id, "c")).futureValue
      val multiResponse   = (actorRef ? MultiRequest(UUID.randomUUID())).futureValue
      val setResponse2    = (actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue
      val setResponse3    = (actorRef ? SetRequest(UUID.randomUUID, id, "b")).futureValue
      val getResponse1    = (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue
      val discardResponse = (actorRef ? DiscardRequest(UUID.randomUUID())).futureValue
      val getResponse2 = (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue
        .asInstanceOf[GetSucceeded]
      assert(setResponse1.isInstanceOf[SetSucceeded])
      assert(multiResponse.isInstanceOf[MultiSucceeded])
      assert(setResponse2.isInstanceOf[SetSuspended])
      assert(setResponse3.isInstanceOf[SetSuspended])
      assert(getResponse1.isInstanceOf[GetSuspended])
      assert(discardResponse.isInstanceOf[DiscardSucceeded])
      assert(getResponse2.value.contains("c"))
      actorRef ! PoisonPill
    }
  }

  describe("KeyOperations") {
    it("should be able to DEL") {
      val actorRef     = system.actorOf(props)
      val id           = UUID.randomUUID().toString
      val setResponse1 = (actorRef ? SetRequest(UUID.randomUUID, id, "a")).futureValue
      val getResponse1 = (actorRef ? GetRequest(UUID.randomUUID, id)).futureValue.asInstanceOf[GetSucceeded]
      val delResponse  = (actorRef ? DelRequest(UUID.randomUUID(), id)).futureValue.asInstanceOf[DelSucceeded]
      assert(setResponse1.isInstanceOf[SetSucceeded])
      assert(getResponse1.value.contains("a"))
      assert(delResponse.value == 1)
      actorRef ! PoisonPill
    }
    it("should be able to DUMP") {
      val actorRef     = system.actorOf(props)
      val id           = UUID.randomUUID().toString
      val setResponse1 = (actorRef ? SetRequest(UUID.randomUUID, id, "abc")).futureValue
      val dumpResponse = (actorRef ? DumpRequest(UUID.randomUUID(), id)).futureValue.asInstanceOf[DumpSucceeded]
      assert(setResponse1.isInstanceOf[SetSucceeded])
      assert(dumpResponse.value.nonEmpty)
      val result =
        dumpResponse.value
          .map(_.toInt)
          .toList
          .zip(List(0, 3, 97, 98, 99, 7, 0, 38, -98, -27, -50))
          .forall {
            case (l, r) =>
              l == r
          }
      assert(result)
      actorRef ! PoisonPill
    }
  }
}
