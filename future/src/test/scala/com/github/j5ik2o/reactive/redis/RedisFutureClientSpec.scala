package com.github.j5ik2o.reactive.redis

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.Options.StartAndEnd
import com.github.j5ik2o.reactive.redis.StringsOperations.{ BitFieldRequest, BitOpRequest }
import com.github.j5ik2o.reactive.redis.StringsOperations.BitFieldRequest.SingedBitType
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

class RedisFutureClientSpec
    extends ActorSpec(ActorSystem("RedisClientSpec"))
    with RedisServerSupport
    with ScalaFutures {

  val idGenerator = new AtomicLong()

  import system.dispatcher

  describe("RedisClient") {
    // --- APPEND
    it("should be able to APPEND") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key = UUID.randomUUID().toString
      val result = (for {
        r1 <- redisClient.append(key, "1")
        r2 <- redisClient.append(key, "2")
        r3 <- redisClient.append(key, "3")
        r4 <- redisClient.get(key)
      } yield (r1, r2, r3, r4)).futureValue
      assert(result._1.contains(1))
      assert(result._2.contains(2))
      assert(result._3.contains(3))
      assert(result._4.contains("123"))
    }
    // --- BITCOUNT
    it("should be able to BITCOUNT") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "a"
      val result = (for {
        _      <- redisClient.set(key, value)
        result <- redisClient.bitCount(key)
      } yield result).futureValue
      assert(result.contains(3))
    }
    // --- BITFIELD
    it("should be able to BITFIELD") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "a"
      val result = (for {
        result <- redisClient.bitField(key, BitFieldRequest.IncrBy(SingedBitType(5), 100, 1))
      } yield result).futureValue
      assert(result.contains(List(1)))
    }
    // --- BITOP
    it("should be able to BITOP") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key1 = UUID.randomUUID().toString
      val key2 = UUID.randomUUID().toString
      val key3 = UUID.randomUUID().toString
      val result = (for {
        _      <- redisClient.set(key1, "foobar")
        _      <- redisClient.set(key2, "abcdef")
        result <- redisClient.bitOp(BitOpRequest.Operand.AND, key3, key1, key2)
      } yield result).futureValue
      assert(result.contains(6))
    }
    // --- BITPOS
    it("should be able to BITPOS") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key = UUID.randomUUID().toString
      val result = (for {
        _      <- redisClient.set(key, """\xff\xf0\x00""")
        result <- redisClient.bitPos(key, 0)
      } yield result).futureValue
      assert(result.contains(12))
    }
    // --- DECR
    it("should be able to DECR") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "1"
      val result = (for {
        _      <- redisClient.set(key, value)
        result <- redisClient.descr(key)
      } yield result).futureValue
      assert(result.contains(value.toInt - 1))
    }
    // --- DECRBY
    it("should be able to DECRBY") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "1"
      val result = (for {
        _      <- redisClient.set(key, value)
        result <- redisClient.descrBy(key, 2)
      } yield result).futureValue
      assert(result.contains(value.toInt - 2))
    }
    // --- GET
    it("should be able to GET") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString

      val result = (for {
        _      <- redisClient.set(key, value)
        result <- redisClient.get(key)
      } yield result).futureValue
      assert(result.contains(value))
    }
    // --- GETBIT
    it("should be able to GETBIT") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString

      val result = (for {
        r <- redisClient.getBit(key, 1)
      } yield r).futureValue
      assert(result.contains(0))
    }
    // --- GETRANGE
    it("should be able to GETRANGE") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key = UUID.randomUUID().toString
      val result = (for {
        _      <- redisClient.set(key, "This is a string")
        result <- redisClient.getRange(key, StartAndEnd(0, 3))
      } yield result).futureValue
      assert(result.contains("This"))
    }
    // --- GETSET
    it("should be able to GETSET") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key    = UUID.randomUUID().toString
      val value1 = UUID.randomUUID().toString
      val value2 = UUID.randomUUID().toString

      val result = (for {
        _       <- redisClient.set(key, value1)
        result1 <- redisClient.getSet(key, value2)
        result2 <- redisClient.get(key)
      } yield (result1, result2)).futureValue
      assert(result._1.contains(value1))
      assert(result._2.contains(value2))
    }
    // --- INCR
    it("should be able to INCR") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "1"
      val result = (for {
        _      <- redisClient.set(key, value)
        result <- redisClient.incr(key)
      } yield result).futureValue
      assert(result.contains(value.toInt + 1))
    }
    // --- INCRBY
    it("should be able to INCRBY") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "1"
      val result = (for {
        _      <- redisClient.set(key, value)
        result <- redisClient.incrBy(key, 4)
      } yield result).futureValue
      assert(result.contains(value.toInt + 4))
    }
    // --- INCRBYFLOAT
    it("should be able to INCRBYFLOAT") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "1"
      val result = (for {
        _      <- redisClient.set(key, value)
        result <- redisClient.incrByFloat(key, 0.1)
      } yield result).futureValue
      assert(result.contains(value.toInt + 0.1))
    }
    // --- MGET
    it("should be able to MGET") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key1   = UUID.randomUUID().toString
      val value1 = UUID.randomUUID().toString
      val key2   = UUID.randomUUID().toString
      val value2 = UUID.randomUUID().toString
      val key3   = UUID.randomUUID().toString

      val result = (for {
        _      <- redisClient.set(key1, value1)
        _      <- redisClient.set(key2, value2)
        result <- redisClient.mGet(Seq(key1, key2, key3))
      } yield result).futureValue
      assert(result.contains(Seq(Some(value1), Some(value2), None)))
    }
    // --- MSET
    it("should be able to MSET") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key1   = UUID.randomUUID().toString
      val value1 = UUID.randomUUID().toString
      val key2   = UUID.randomUUID().toString
      val value2 = UUID.randomUUID().toString
      val key3   = UUID.randomUUID().toString

      val result = (for {
        _      <- redisClient.mSet(Map(key1 -> value1, key2 -> value2))
        result <- redisClient.mGet(Seq(key1, key2, key3))
      } yield result).futureValue
      assert(result.contains(Seq(Some(value1), Some(value2), None)))
    }
    // --- MSETNX
    it("should be able to MSETNX") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key1   = UUID.randomUUID().toString
      val value1 = UUID.randomUUID().toString
      val key2   = UUID.randomUUID().toString
      val value2 = UUID.randomUUID().toString
      val key3   = UUID.randomUUID().toString

      val result = (for {
        _      <- redisClient.mSetNx(Map(key1 -> value1, key2 -> value2))
        _      <- redisClient.mSetNx(Map(key1 -> UUID.randomUUID().toString, key2 -> UUID.randomUUID().toString))
        result <- redisClient.mGet(Seq(key1, key2, key3))
      } yield result).futureValue
      assert(result.contains(Seq(Some(value1), Some(value2), None)))
    }
    // --- PSETEX
    it("should be able to PSETEX") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString
      val result = (for {
        _ <- redisClient.pSetEx(key, 1 seconds, value)
        r <- {
          Thread.sleep(1500)
          redisClient.get(key)
        }
      } yield r).futureValue
      assert(result.isEmpty)
    }
    // --- SET
    it("should be able to SET") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString
      val result = (for {
        _ <- redisClient.set(key, value)
      } yield ()).futureValue
    }
    // --- SETBIT
    it("should be able to SETBIT") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString
      val result = (for {
        r1 <- redisClient.setBit(key, 7, 1)
        r2 <- redisClient.setBit(key, 7, 0)
        r3 <- redisClient.get(key)
      } yield (r1, r2, r3)).futureValue
      assert(result._1.contains(0))
      assert(result._2.contains(1))
      assert(result._3.contains("\u0000"))
    }
    // --- SETEX
    it("should be able to SETEX") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString
      val result = (for {
        _ <- redisClient.setEx(key, 1 seconds, value)
        r <- {
          Thread.sleep(1500)
          redisClient.get(key)
        }
      } yield r).futureValue
      assert(result.isEmpty)
    }
    // --- SETNX
    it("should be able to SETNX") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString
      val result = (for {
        _ <- redisClient.setNx(key, value)
        _ <- redisClient.setNx(key, UUID.randomUUID().toString)
        r <- redisClient.get(key)
      } yield r).futureValue
      assert(result.contains(value))
    }
    // --- SETRANGE
    it("should be able to SETRANGE") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "Hello World"
      val result = (for {
        _ <- redisClient.set(key, value)
        _ <- redisClient.setRange(key, 6, "Redis")
        r <- redisClient.get(key)
      } yield r).futureValue
      assert(result.contains("Hello Redis"))
    }
    // --- STRLEN
    it("should be able to STRLEN") {
      val redisClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "12345"
      val result = (for {
        _ <- redisClient.set(key, value)
        r <- redisClient.strlen(key)
      } yield r).futureValue
      assert(result.contains(5))
    }
  }

}
