package com.github.j5ik2o.reactive.redis.feature

import java.util.UUID

import akka.actor.ActorSystem
import akka.routing.DefaultResizer
import cats.data.NonEmptyList
import cats.implicits._
import com.github.j5ik2o.reactive.redis.command.strings.BitFieldRequest.SingedBitType
import com.github.j5ik2o.reactive.redis.command.strings.{ BitFieldRequest, BitOpRequest, BitPosRequest, StartAndEnd }
import com.github.j5ik2o.reactive.redis.util.BitUtil
import com.github.j5ik2o.reactive.redis.{ AbstractRedisClientSpec, PeerConfig, RedisConnection, RedisConnectionPool }
import monix.eval.Task
import monix.execution.Scheduler
import org.scalacheck.Shrink

class StringsFeatureSpec extends AbstractRedisClientSpec(ActorSystem("StringsFeatureSpec")) {

  implicit val noShrink: Shrink[String] = Shrink.shrinkAny

  override protected def createConnectionPool(peerConfigs: NonEmptyList[PeerConfig]): RedisConnectionPool[Task] =
    RedisConnectionPool.ofMultipleRoundRobin(sizePerPeer = 10,
                                             peerConfigs,
                                             RedisConnection(_, _),
                                             reSizer = Some(DefaultResizer(lowerBound = 5, upperBound = 15)))

  "StringsFeature" - {
    "append" in forAll(keyStrValueGen) {
      case (k, v) =>
        val result = runProgram(for {
          ar1 <- redisClient.append(k, v)
          gr1 <- redisClient.get(k)
          ar2 <- redisClient.append(k, v)
          gr2 <- redisClient.get(k)
        } yield (ar1, ar2, gr1, gr2))

        result._1.value shouldBe v.length
        result._2.value shouldBe v.length * 2
        result._3.value shouldBe Some(v)
        result._4.value shouldBe Some(v + v)
    }
    "bitcount" in forAll(keyStrValueGen) {
      case (k, v) =>
        val end              = v.length / 2
        val expectedBitCount = BitUtil.getBitCount(v, Some(StartAndEnd(0, end)))
        val result = runProgram(for {
          _  <- redisClient.set(k, v)
          br <- redisClient.bitCount(k, startAndEnd = Some(StartAndEnd(0, end)))
        } yield br)
        result.value shouldBe expectedBitCount
    }
    "bitField" in {
      val k = UUID.randomUUID().toString

      val result = runProgram(for {
        br <- redisClient.bitField(k, BitFieldRequest.IncrBy(SingedBitType(5), 100, 1))
      } yield br)

      result.value shouldBe List(1)
    }
    "bitOp" in {
      val k1 = UUID.randomUUID().toString
      val k2 = UUID.randomUUID().toString

      val result = runProgram(for {
        _  <- redisClient.set(k1, "foobar")
        _  <- redisClient.set(k2, "abcdef")
        br <- redisClient.bitOp(BitOpRequest.Operand.AND, "dest", k1, k2)
        gr <- redisClient.get("dest")
      } yield (br, gr))

      result._1.value shouldBe 6
      result._2.value shouldBe Some("`bc`ab")
    }
    "bitPos" in {
      val k = UUID.randomUUID().toString

      val result = runProgram(for {
        _   <- redisClient.set(k, "\\xff\\xf0\\x00")
        br1 <- redisClient.bitPos(k, 0)
        _   <- redisClient.set(k, "\\x00\\xff\\xf0")
        br2 <- redisClient.bitPos(k, 1, Some(BitPosRequest.StartAndEnd(0)))
        br3 <- redisClient.bitPos(k, 1, Some(BitPosRequest.StartAndEnd(2)))
        _   <- redisClient.set(k, "\\x00\\x00\\x00")
        br4 <- redisClient.bitPos(k, 1, None)
      } yield (br1, br2, br3, br4))

      result._1.value shouldBe 12
      result._2.value shouldBe 8
      result._3.value shouldBe 16
      result._4.value shouldBe -1
    }
    "decr" in forAll(keyNumValueGen) {
      case (k, v) =>
        val result = runProgram(for {
          _  <- redisClient.set(k, v)
          gr <- redisClient.decr(k)
        } yield gr)

        result.value shouldBe (v - 1)
    }
    "decrBy" in forAll(keyNumValueGen) {
      case (k, v) =>
        val result = runProgram(for {
          _  <- redisClient.set(k, v)
          gr <- redisClient.decrBy(k, 3)
        } yield gr)

        result.value shouldBe (v - 3)
    }
    "get" in forAll(keyStrValueGen) {
      case (k, v) =>
        val result = runProgram(for {
          _      <- redisClient.set(k, v)
          result <- redisClient.get(k)
        } yield result)

        result.value shouldBe Some(v)
    }
    "getBit" in {
      val k = UUID.randomUUID().toString
      val result = runProgram(for {
        _   <- redisClient.setBit(k, 7, 1)
        gr1 <- redisClient.getBit(k, 0)
        gr2 <- redisClient.getBit(k, 7)
        gr3 <- redisClient.getBit(k, 100)
      } yield (gr1, gr2, gr3))

      result._1.value shouldBe 0
      result._2.value shouldBe 1
      result._3.value shouldBe 0
    }
    "getRange" in {
      val k = UUID.randomUUID().toString
      val result = runProgram(for {
        _   <- redisClient.set(k, "This is a string")
        gr1 <- redisClient.getRange(k, StartAndEnd(0, 3))
        gr2 <- redisClient.getRange(k, StartAndEnd(-3, -1))
        gr3 <- redisClient.getRange(k, StartAndEnd(0, -1))
        gr4 <- redisClient.getRange(k, StartAndEnd(10, 100))
      } yield (gr1, gr2, gr3, gr4))

      result._1.value shouldBe Some("This")
      result._2.value shouldBe Some("ing")
      result._3.value shouldBe Some("This is a string")
      result._4.value shouldBe Some("string")
    }
    "getSet" in {
      val k = UUID.randomUUID().toString
      val result = runProgram(for {
        _   <- redisClient.incr(k)
        gr1 <- redisClient.getSet(k, "0")
        gr2 <- redisClient.get(k)
      } yield (gr1, gr2))

      result._1.value shouldBe Some("1")
      result._2.value shouldBe Some("0")
    }
    "incr" in {}
    "incrBy" in {}
    "incrByFloat" in {}
    "mget" in {}
    "mset" in {}
    "msetNx" in {}
    "psetEx" in {}
    "set" in {}
    "setBit" in {}
    "setEx" in {}
    "setNx" in {}
    "setRange" in {}
    "strLen" in {}

  }

}
