package com.github.j5ik2o.reactive.redis

import java.util.UUID

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.Options.StartAndEnd
import com.github.j5ik2o.reactive.redis.StringsOperations.BitFieldRequest.SingedBitType
import com.github.j5ik2o.reactive.redis.StringsOperations.{ BitFieldRequest, BitOpRequest }

import scala.concurrent.duration._

class RedisFutureClientSpec extends ActorSpec(ActorSystem("RedisFutureClientSpec")) with RedisServerSupport {

  import system.dispatcher

  describe("redisFutureClient") {
    // --- APPEND
    it("should be able to APPEND") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key = UUID.randomUUID().toString
      val result = (for {
        r1 <- redisFutureClient.append(key, "1")
        r2 <- redisFutureClient.append(key, "2")
        r3 <- redisFutureClient.append(key, "3")
        r4 <- redisFutureClient.get(key)
      } yield (r1, r2, r3, r4)).futureValue
      assert(result._1.contains(1))
      assert(result._2.contains(2))
      assert(result._3.contains(3))
      assert(result._4.contains("123"))
      redisFutureClient.dispose()
    }
    // --- BITCOUNT
    it("should be able to BITCOUNT") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "a"
      val result = (for {
        _      <- redisFutureClient.set(key, value)
        result <- redisFutureClient.bitCount(key)
      } yield result).futureValue
      assert(result.contains(3))
      redisFutureClient.dispose()
    }
    // --- BITFIELD
    it("should be able to BITFIELD") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "a"
      val result = (for {
        result <- redisFutureClient.bitField(key, BitFieldRequest.IncrBy(SingedBitType(5), 100, 1))
      } yield result).futureValue
      assert(result.contains(List(1)))
      redisFutureClient.dispose()
    }
    // --- BITOP
    it("should be able to BITOP") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key1 = UUID.randomUUID().toString
      val key2 = UUID.randomUUID().toString
      val key3 = UUID.randomUUID().toString
      val result = (for {
        _      <- redisFutureClient.set(key1, "foobar")
        _      <- redisFutureClient.set(key2, "abcdef")
        result <- redisFutureClient.bitOp(BitOpRequest.Operand.AND, key3, key1, key2)
      } yield result).futureValue
      assert(result.contains(6))
      redisFutureClient.dispose()
    }
    // --- BITPOS
    it("should be able to BITPOS") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key = UUID.randomUUID().toString
      val result = (for {
        _      <- redisFutureClient.set(key, """\xff\xf0\x00""")
        result <- redisFutureClient.bitPos(key, 0)
      } yield result).futureValue
      assert(result.contains(12))
      redisFutureClient.dispose()
    }
    // --- DECR
    it("should be able to DECR") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "1"
      val result = (for {
        _      <- redisFutureClient.set(key, value)
        result <- redisFutureClient.descr(key)
      } yield result).futureValue
      assert(result.contains(value.toInt - 1))
      redisFutureClient.dispose()
    }
    // --- DECRBY
    it("should be able to DECRBY") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "1"
      val result = (for {
        _      <- redisFutureClient.set(key, value)
        result <- redisFutureClient.descrBy(key, 2)
      } yield result).futureValue
      assert(result.contains(value.toInt - 2))
      redisFutureClient.dispose()
    }
    // --- GET
    it("should be able to GET") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString

      val result = (for {
        _      <- redisFutureClient.set(key, value)
        result <- redisFutureClient.get(key)
      } yield result).futureValue
      assert(result.contains(value))
      redisFutureClient.dispose()
    }
    // --- GETBIT
    it("should be able to GETBIT") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString

      val result = (for {
        r <- redisFutureClient.getBit(key, 1)
      } yield r).futureValue
      assert(result.contains(0))
      redisFutureClient.dispose()
    }
    // --- GETRANGE
    it("should be able to GETRANGE") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key = UUID.randomUUID().toString
      val result = (for {
        _      <- redisFutureClient.set(key, "This is a string")
        result <- redisFutureClient.getRange(key, StartAndEnd(0, 3))
      } yield result).futureValue
      assert(result.contains("This"))
      redisFutureClient.dispose()
    }
    // --- GETSET
    it("should be able to GETSET") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key    = UUID.randomUUID().toString
      val value1 = UUID.randomUUID().toString
      val value2 = UUID.randomUUID().toString

      val result = (for {
        _       <- redisFutureClient.set(key, value1)
        result1 <- redisFutureClient.getSet(key, value2)
        result2 <- redisFutureClient.get(key)
      } yield (result1, result2)).futureValue
      assert(result._1.contains(value1))
      assert(result._2.contains(value2))
      redisFutureClient.dispose()
    }
    // --- INCR
    it("should be able to INCR") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "1"
      val result = (for {
        _      <- redisFutureClient.set(key, value)
        result <- redisFutureClient.incr(key)
      } yield result).futureValue
      assert(result.contains(value.toInt + 1))
      redisFutureClient.dispose()
    }
    // --- INCRBY
    it("should be able to INCRBY") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "1"
      val result = (for {
        _      <- redisFutureClient.set(key, value)
        result <- redisFutureClient.incrBy(key, 4)
      } yield result).futureValue
      assert(result.contains(value.toInt + 4))
      redisFutureClient.dispose()
    }
    // --- INCRBYFLOAT
    it("should be able to INCRBYFLOAT") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "1"
      val result = (for {
        _      <- redisFutureClient.set(key, value)
        result <- redisFutureClient.incrByFloat(key, 0.1)
      } yield result).futureValue
      assert(result.contains(value.toInt + 0.1))
      redisFutureClient.dispose()
    }
    // --- MGET
    it("should be able to MGET") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key1   = UUID.randomUUID().toString
      val value1 = UUID.randomUUID().toString
      val key2   = UUID.randomUUID().toString
      val value2 = UUID.randomUUID().toString
      val key3   = UUID.randomUUID().toString

      val result = (for {
        _      <- redisFutureClient.set(key1, value1)
        _      <- redisFutureClient.set(key2, value2)
        result <- redisFutureClient.mGet(Seq(key1, key2, key3))
      } yield result).futureValue
      assert(result.contains(Seq(Some(value1), Some(value2), None)))
      redisFutureClient.dispose()
    }
    // --- MSET
    it("should be able to MSET") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key1   = UUID.randomUUID().toString
      val value1 = UUID.randomUUID().toString
      val key2   = UUID.randomUUID().toString
      val value2 = UUID.randomUUID().toString
      val key3   = UUID.randomUUID().toString

      val result = (for {
        _      <- redisFutureClient.mSet(Map(key1 -> value1, key2 -> value2))
        result <- redisFutureClient.mGet(Seq(key1, key2, key3))
      } yield result).futureValue
      assert(result.contains(Seq(Some(value1), Some(value2), None)))
      redisFutureClient.dispose()
    }
    // --- MSETNX
    it("should be able to MSETNX") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key1   = UUID.randomUUID().toString
      val value1 = UUID.randomUUID().toString
      val key2   = UUID.randomUUID().toString
      val value2 = UUID.randomUUID().toString
      val key3   = UUID.randomUUID().toString

      val result = (for {
        _      <- redisFutureClient.mSetNx(Map(key1 -> value1, key2 -> value2))
        _      <- redisFutureClient.mSetNx(Map(key1 -> UUID.randomUUID().toString, key2 -> UUID.randomUUID().toString))
        result <- redisFutureClient.mGet(Seq(key1, key2, key3))
      } yield result).futureValue
      assert(result.contains(Seq(Some(value1), Some(value2), None)))
      redisFutureClient.dispose()
    }
    // --- PSETEX
    it("should be able to PSETEX") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString
      val result = (for {
        _ <- redisFutureClient.pSetEx(key, 1 seconds, value)
        r <- {
          Thread.sleep(1500)
          redisFutureClient.get(key)
        }
      } yield r).futureValue
      assert(result.isEmpty)
      redisFutureClient.dispose()
    }
    // --- SET
    it("should be able to SET") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString
      val result = (for {
        _ <- redisFutureClient.set(key, value)
      } yield ()).futureValue
      redisFutureClient.dispose()
    }
    // --- SETBIT
    it("should be able to SETBIT") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString
      val result = (for {
        r1 <- redisFutureClient.setBit(key, 7, 1)
        r2 <- redisFutureClient.setBit(key, 7, 0)
        r3 <- redisFutureClient.get(key)
      } yield (r1, r2, r3)).futureValue
      assert(result._1.contains(0))
      assert(result._2.contains(1))
      assert(result._3.contains("\u0000"))
      redisFutureClient.dispose()
    }
    // --- SETEX
    it("should be able to SETEX") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString
      val result = (for {
        _ <- redisFutureClient.setEx(key, 1 seconds, value)
        r <- {
          Thread.sleep(1500)
          redisFutureClient.get(key)
        }
      } yield r).futureValue
      assert(result.isEmpty)
      redisFutureClient.dispose()
    }
    // --- SETNX
    it("should be able to SETNX") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString
      val result = (for {
        _ <- redisFutureClient.setNx(key, value)
        _ <- redisFutureClient.setNx(key, UUID.randomUUID().toString)
        r <- redisFutureClient.get(key)
      } yield r).futureValue
      assert(result.contains(value))
      redisFutureClient.dispose()
    }
    // --- SETRANGE
    it("should be able to SETRANGE") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "Hello World"
      val result = (for {
        _ <- redisFutureClient.set(key, value)
        _ <- redisFutureClient.setRange(key, 6, "Redis")
        r <- redisFutureClient.get(key)
      } yield r).futureValue
      assert(result.contains("Hello Redis"))
      redisFutureClient.dispose()
    }
    // --- STRLEN
    it("should be able to STRLEN") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "12345"
      val result = (for {
        _ <- redisFutureClient.set(key, value)
        r <- redisFutureClient.strlen(key)
      } yield r).futureValue
      assert(result.contains(5))
      redisFutureClient.dispose()
    }
  }

}
