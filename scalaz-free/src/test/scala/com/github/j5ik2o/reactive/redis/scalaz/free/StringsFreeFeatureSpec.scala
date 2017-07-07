package com.github.j5ik2o.reactive.redis.scalaz.free

import java.util.UUID

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.Options.StartAndEnd
import com.github.j5ik2o.reactive.redis.StringsOperations.BitFieldRequest.SingedBitType
import com.github.j5ik2o.reactive.redis.StringsOperations.{ BitFieldRequest, BitOpRequest }
import com.github.j5ik2o.reactive.redis.{ ActorSpec, RedisFutureClient, RedisServerSupport }

import scala.concurrent.duration._
import scalaz.Free
import scalaz.std.scalaFuture._

class StringsFreeFeatureSpec
    extends ActorSpec(ActorSystem("StringsFreeFeatureSpec"))
    with StringsFreeFeature
    with RedisServerSupport {

  import system.dispatcher

  def getResult[A](redisFutureClient: RedisFutureClient, program: Free[StringsDSL, A]) = {
    val interpreter = new StringsInterpreter(redisFutureClient)
    val future      = program.foldMap(interpreter)
    future.futureValue
  }

  describe("StringsFreeFeature") {
    // --- APPEND
    it("should be able to APPEND") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key = UUID.randomUUID().toString
      val program = for {
        r1 <- append(key, "Hello")
        r2 <- append(key, ", ")
        r3 <- append(key, "World!")
        r4 <- get(key)
      } yield (r1, r2, r3, r4)
      val result = getResult(redisFutureClient, program)
      assert(result._1.contains(5))
      assert(result._2.contains(7))
      assert(result._3.contains(13))
      assert(result._4.contains("Hello, World!"))
      redisFutureClient.dispose()
    }
    // --- BITCOUNT
    it("should be able to BITCOUNT") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key = UUID.randomUUID().toString
      val program = for {
        _ <- set(key, "a")
        n <- bitCount(key)
      } yield n
      val result = getResult(redisFutureClient, program)
      assert(result.contains(3))
      redisFutureClient.dispose()
      redisFutureClient.dispose()
    }
    // --- BITFIELD
    it("should be able to BITFIELD") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "a"
      val program = for {
        result <- bitField(key, BitFieldRequest.IncrBy(SingedBitType(5), 100, 1))
      } yield result
      val result = getResult(redisFutureClient, program)
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
      val program = for {
        _      <- set(key1, "foobar")
        _      <- set(key2, "abcdef")
        result <- bitOp(BitOpRequest.Operand.AND, key3, key1, key2)
      } yield result
      val result = getResult(redisFutureClient, program)
      assert(result.contains(6))
      redisFutureClient.dispose()
    }
    // --- BITPOS
    it("should be able to BITPOS") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key = UUID.randomUUID().toString
      val program = for {
        _      <- set(key, """\xff\xf0\x00""")
        result <- bitPos(key, 0)
      } yield result
      val result = getResult(redisFutureClient, program)
      assert(result.contains(12))
      redisFutureClient.dispose()
    }
    // --- DECR
    it("should be able to DECR") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "1"
      val program = for {
        _      <- set(key, value)
        result <- descr(key)
      } yield result
      val result = getResult(redisFutureClient, program)
      assert(result.contains(value.toInt - 1))
      redisFutureClient.dispose()
    }
    // --- DECRBY
    it("should be able to DECRBY") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "1"
      val program = for {
        _      <- set(key, value)
        result <- descrBy(key, 2)
      } yield result
      val result = getResult(redisFutureClient, program)
      assert(result.contains(value.toInt - 2))
      redisFutureClient.dispose()
    }
    // --- GET
    it("should be able to GET") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString

      val program = for {
        _      <- set(key, value)
        result <- get(key)
      } yield result
      val result = getResult(redisFutureClient, program)
      assert(result.contains(value))
      redisFutureClient.dispose()
    }
    // --- GETBIT
    it("should be able to GETBIT") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString

      val program = for {
        r <- getBit(key, 1)
      } yield r
      val result = getResult(redisFutureClient, program)
      assert(result.contains(0))
      redisFutureClient.dispose()
    }
    // --- GETRANGE
    it("should be able to GETRANGE") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key = UUID.randomUUID().toString
      val program = for {
        _      <- set(key, "This is a string")
        result <- getRange(key, StartAndEnd(0, 3))
      } yield result
      val result = getResult(redisFutureClient, program)
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

      val program = for {
        _       <- set(key, value1)
        result1 <- getSet(key, value2)
        result2 <- get(key)
      } yield (result1, result2)
      val result = getResult(redisFutureClient, program)
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
      val program = for {
        _      <- set(key, value)
        result <- incr(key)
      } yield result
      val result = getResult(redisFutureClient, program)
      assert(result.contains(value.toInt + 1))
      redisFutureClient.dispose()
    }
    // --- INCRBY
    it("should be able to INCRBY") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "1"
      val program = for {
        _      <- set(key, value)
        result <- incrBy(key, 4)
      } yield result
      val result = getResult(redisFutureClient, program)
      assert(result.contains(value.toInt + 4))
      redisFutureClient.dispose()
    }
    // --- INCRBYFLOAT
    it("should be able to INCRBYFLOAT") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "1"
      val program = for {
        _      <- set(key, value)
        result <- incrByFloat(key, 0.1)
      } yield result
      val result = getResult(redisFutureClient, program)
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

      val program = for {
        _      <- set(key1, value1)
        _      <- set(key2, value2)
        result <- mGet(Seq(key1, key2, key3))
      } yield result
      val result = getResult(redisFutureClient, program)
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

      val program = for {
        _      <- mSet(Map(key1 -> value1, key2 -> value2))
        result <- mGet(Seq(key1, key2, key3))
      } yield result
      val result = getResult(redisFutureClient, program)
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

      val program = for {
        _      <- mSetNx(Map(key1 -> value1, key2 -> value2))
        _      <- mSetNx(Map(key1 -> UUID.randomUUID().toString, key2 -> UUID.randomUUID().toString))
        result <- mGet(Seq(key1, key2, key3))
      } yield result
      val result = getResult(redisFutureClient, program)
      assert(result.contains(Seq(Some(value1), Some(value2), None)))
      redisFutureClient.dispose()
    }
    // --- PSETEX
    it("should be able to PSETEX") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString
      val program = for {
        _ <- pSetEx(key, 1 seconds, value)
        r <- {
          Thread.sleep(1500)
          get(key)
        }
      } yield r
      val result = getResult(redisFutureClient, program)
      assert(result.isEmpty)
      redisFutureClient.dispose()
    }
    // --- SET
    it("should be able to SET") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString
      val program = for {
        _ <- set(key, value)
      } yield ()
      getResult(redisFutureClient, program)
      redisFutureClient.dispose()
    }
    // --- SETBIT
    it("should be able to SETBIT") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString
      val program = for {
        r1 <- setBit(key, 7, 1)
        r2 <- setBit(key, 7, 0)
        r3 <- get(key)
      } yield (r1, r2, r3)
      val result = getResult(redisFutureClient, program)
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
      val program = for {
        _ <- setEx(key, 1 seconds, value)
        r <- {
          Thread.sleep(1500)
          get(key)
        }
      } yield r
      val result = getResult(redisFutureClient, program)
      assert(result.isEmpty)
      redisFutureClient.dispose()
    }
    // --- SETNX
    it("should be able to SETNX") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString
      val program = for {
        _ <- setNx(key, value)
        _ <- setNx(key, UUID.randomUUID().toString)
        r <- get(key)
      } yield r
      val result = getResult(redisFutureClient, program)
      assert(result.contains(value))
      redisFutureClient.dispose()
    }
    // --- SETRANGE
    it("should be able to SETRANGE") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "Hello World"
      val program = for {
        _ <- set(key, value)
        _ <- setRange(key, 6, "Redis")
        r <- get(key)
      } yield r
      val result = getResult(redisFutureClient, program)
      assert(result.contains("Hello Redis"))
      redisFutureClient.dispose()
    }
    // --- STRLEN
    it("should be able to STRLEN") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key   = UUID.randomUUID().toString
      val value = "12345"
      val program = for {
        _ <- set(key, value)
        r <- strlen(key)
      } yield r
      val result = getResult(redisFutureClient, program)
      assert(result.contains(5))
      redisFutureClient.dispose()
    }
  }

}
