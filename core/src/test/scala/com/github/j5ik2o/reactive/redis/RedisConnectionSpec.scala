package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.util.UUID

import akka.actor.ActorSystem
import akka.routing.DefaultResizer
import cats.data.NonEmptyList
import cats.implicits._
import com.github.j5ik2o.reactive.redis.command.keys.{ KeysRequest, KeysSucceeded }
import com.github.j5ik2o.reactive.redis.command.strings.BitFieldRequest.SingedBitType
import com.github.j5ik2o.reactive.redis.command.strings._
import com.github.j5ik2o.reactive.redis.command.transactions.{
  ExecRequest,
  ExecSucceeded,
  MultiRequest,
  MultiSucceeded
}
import com.github.j5ik2o.reactive.redis.util.BitUtil
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Shrink

class RedisConnectionSpec extends AbstractActorSpec(ActorSystem("RedisConnectionSpec")) {
  implicit val noShrink: Shrink[String] = Shrink.shrinkAny

  var connection: RedisConnection = _
  val redisClient: RedisClient    = RedisClient()

  override protected def createConnectionPool(peerConfigs: NonEmptyList[PeerConfig]): RedisConnectionPool[Task] =
    RedisConnectionPool.ofMultipleRoundRobin(sizePerPeer = 10,
                                             peerConfigs,
                                             RedisConnection(_, _),
                                             reSizer = Some(DefaultResizer(lowerBound = 5, upperBound = 15)))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    connection = RedisConnection(
      PeerConfig(new InetSocketAddress("127.0.0.1", redisMasterServer.getPort),
                 backoffConfig = BackoffConfig(maxRestarts = 1)),
      None
    )
  }

  override protected def afterAll(): Unit = {
    connection.shutdown()
    super.afterAll()
  }

  "redisclient" - {
    "append" in {
      val key = UUID.randomUUID().toString
      connection
        .send(AppendRequest(UUID.randomUUID(), key, "a"))
        .runAsync
        .futureValue
        .asInstanceOf[AppendSucceeded]
        .value shouldBe 1
      connection
        .send(AppendRequest(UUID.randomUUID(), key, "b"))
        .runAsync
        .futureValue
        .asInstanceOf[AppendSucceeded]
        .value shouldBe 2
      connection
        .send(AppendRequest(UUID.randomUUID(), key, "c"))
        .runAsync
        .futureValue
        .asInstanceOf[AppendSucceeded]
        .value shouldBe 3
      val result =
        redisClient.get(key).run(connection).runAsync.futureValue
      result.value shouldBe Some("abc")
    }
    "bitcount" in forAll(keyStrValueGen) {
      case (k, v) =>
        redisClient.set(k, v).run(connection).runAsync.futureValue
        val end = v.length / 2
        val result2 =
          connection
            .send(BitCountRequest(UUID.randomUUID(), k, startAndEnd = Some(StartAndEnd(0, end))))
            .runAsync
            .futureValue
            .asInstanceOf[BitCountSucceeded]
        result2.value shouldBe BitUtil.getBitCount(v, startAndEnd = Some(StartAndEnd(0, end)))
    }
    "bitfield" in {
      val key = UUID.randomUUID().toString
      val result = connection
        .send(BitFieldRequest(UUID.randomUUID(), key, BitFieldRequest.IncrBy(SingedBitType(5), 100, 1)))
        .runAsync
        .futureValue
        .asInstanceOf[BitFieldSucceeded]
      result.values shouldBe List(1)
    }
    "bitop" in {
      val key1 = UUID.randomUUID().toString
      val key2 = UUID.randomUUID().toString
      val key3 = UUID.randomUUID().toString
      redisClient.set(key1, "foobar").run(connection).runAsync.futureValue
      redisClient.set(key2, "abcdef").run(connection).runAsync.futureValue
      val result = connection
        .send(BitOpRequest(UUID.randomUUID(), BitOpRequest.Operand.AND, key3, key1, key2))
        .runAsync
        .futureValue
        .asInstanceOf[BitOpSucceeded]
      val value = redisClient.get(key3).run(connection).runAsync.futureValue
      result.value shouldBe 6
      value.value shouldBe Some("`bc`ab")
    }
    "bitpos" in {
      val key = UUID.randomUUID().toString
      redisClient.set(key, """\xff\xf0\x00""").run(connection).runAsync.futureValue
      val result =
        connection.send(BitPosRequest(UUID.randomUUID(), key, 0)).runAsync.futureValue.asInstanceOf[BitPosSucceeded]
      result.value shouldBe 12
    }
    "decr" in {
      val key = UUID.randomUUID().toString
      redisClient.set(key, 10).run(connection).runAsync.futureValue
      val result = connection.send(DecrRequest(UUID.randomUUID(), key)).runAsync.futureValue.asInstanceOf[DecrSucceeded]
      result.value shouldBe 9
    }
    "decrby" in {
      val key = UUID.randomUUID().toString
      redisClient.set(key, 10).run(connection).runAsync.futureValue
      val result =
        connection.send(DecrByRequest(UUID.randomUUID(), key, 2)).runAsync.futureValue.asInstanceOf[DecrBySucceeded]
      result.value shouldBe 8
    }
    "get" in {
      val key   = UUID.randomUUID().toString
      val value = "1"
      redisClient.set(key, value).run(connection).runAsync.futureValue
      val result2 = connection.send(GetRequest(UUID.randomUUID(), key)).runAsync.futureValue
      result2.isInstanceOf[GetSucceeded] shouldBe true
      result2.asInstanceOf[GetSucceeded].value shouldBe Some(value)
    }
    "getbit" in {
      val key = UUID.randomUUID().toString
      val result =
        connection.send(GetBitRequest(UUID.randomUUID(), key, 1)).runAsync.futureValue.asInstanceOf[GetBitSucceeded]
      result.value shouldBe 0
    }
    "getrange" in {
      val key   = UUID.randomUUID().toString
      val value = "This is a string"
      redisClient.set(key, value).run(connection).runAsync.futureValue
      val result = connection
        .send(GetRangeRequest(UUID.randomUUID(), key, StartAndEnd(0, 3)))
        .runAsync
        .futureValue
        .asInstanceOf[GetRangeSucceeded]
      result.value shouldBe Some("This")
    }
    "getset" in {
      val key   = UUID.randomUUID().toString
      val value = "a"
      redisClient.set(key, value).run(connection).runAsync.futureValue
      val result =
        connection.send(GetSetRequest(UUID.randomUUID(), key, "b")).runAsync.futureValue.asInstanceOf[GetSetSucceeded]
      result.value shouldBe Some("a")
      val result2 = redisClient.get(key).run(connection).runAsync.futureValue
      result2.value shouldBe Some("b")
    }
    "incr" in {}
    "incrby" in {}
    "mget" in {}
    "mset" in {}
    "msetnx" in {}
    "psetex" in {}
    "set" in {}
    "setbit" in {}
    "setex" in {}
    "setnx" in {}
    "setrange" in {}
    "strlen" in {}
    "dump" in {
      val key   = UUID.randomUUID().toString
      val value = "a"
      redisClient.set(key, value).run(connection).runAsync.futureValue
      val result = redisClient.dump(key).run(connection).runAsync.futureValue
      println(result)
    }
    "keys" in {
      redisClient.set("test-1", UUID.randomUUID().toString).run(connection).runAsync.futureValue
      redisClient.set("test-2", UUID.randomUUID().toString).run(connection).runAsync.futureValue
      redisClient.set("test-3", UUID.randomUUID().toString).run(connection).runAsync.futureValue
      val result =
        connection.send(KeysRequest(UUID.randomUUID(), "tst-*")).runAsync.futureValue.asInstanceOf[KeysSucceeded]
      println(result.values)
    }
    "tx" in {
      val resultStart =
        connection.send(MultiRequest(UUID.randomUUID())).runAsync.futureValue.asInstanceOf[MultiSucceeded]
      redisClient.set("test-1", UUID.randomUUID().toString).run(connection).runAsync.futureValue
      redisClient.get("test-1").run(connection).runAsync.futureValue
      val resultFinish =
        connection.send(ExecRequest(UUID.randomUUID())).runAsync.futureValue.asInstanceOf[ExecSucceeded]
      println(resultFinish)
    }
  }
}
