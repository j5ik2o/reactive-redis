package com.github.j5ik2o.reactive.redis

import java.util.UUID

import akka.actor.ActorSystem
import cats.data.ReaderT
import com.github.j5ik2o.reactive.redis.command._
import com.github.j5ik2o.reactive.redis.command.strings.SetRequest.ToString
import com.github.j5ik2o.reactive.redis.command.strings._
import monix.eval.Task

object RedisClient {

  def apply()(implicit system: ActorSystem): RedisClient = new RedisClient()

}

class RedisClient(implicit system: ActorSystem) {

  def send[C <: CommandRequest](cmd: C): ReaderTTaskRedisConnection[cmd.Response] = ReaderT(_.send(cmd))

  def append(key: String, value: String): ReaderT[Task, RedisConnection, Int] =
    send(AppendRequest(UUID.randomUUID(), key, value)).flatMap {
      case AppendSucceeded(_, _, result) => ReaderTTask.pure(result)
      case AppendFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def bitCount(key: String, startAndEnd: Option[StartAndEnd]): ReaderTTaskRedisConnection[Int] =
    send(BitCountRequest(UUID.randomUUID(), key, startAndEnd)).flatMap {
      case BitCountSucceeded(_, _, value) => ReaderTTask.pure(value)
      case BitCountFailed(_, _, ex)       => ReaderTTask.raiseError(ex)
    }

  def bitField(key: String, options: BitFieldRequest.SubOption*): ReaderTTaskRedisConnection[Seq[Int]] =
    send(BitFieldRequest(UUID.randomUUID(), key, options: _*)).flatMap {
      case BitFieldSucceeded(_, _, values) => ReaderTTask.pure(values)
      case BitFieldFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def bitOp(operand: BitOpRequest.Operand,
            outputKey: String,
            inputKey1: String,
            inputKey2: String): ReaderTTaskRedisConnection[Int] =
    send(BitOpRequest(UUID.randomUUID(), operand, outputKey, inputKey1, inputKey2)).flatMap {
      case BitOpSucceeded(_, _, value) => ReaderTTask.pure(value)
      case BitOpFailed(_, _, ex)       => ReaderTTask.raiseError(ex)
    }

  def bitPos(key: String, bit: Int, startAndEnd: Option[StartAndEnd] = None): ReaderTTaskRedisConnection[Int] =
    send(BitPosRequest(UUID.randomUUID(), key, bit, startAndEnd)).flatMap {
      case BitPosSucceeded(_, _, n) => ReaderTTask.pure(n)
      case BitPosFailed(_, _, ex)   => ReaderTTask.raiseError(ex)
    }

  def decr(key: String): ReaderTTaskRedisConnection[Int] = send(DecrRequest(UUID.randomUUID(), key)).flatMap {
    case DecrSucceeded(_, _, value) => ReaderTTask.pure(value)
    case DecrFailed(_, _, ex)       => ReaderTTask.raiseError(ex)
  }

  def decrBy(key: String, value: Int): ReaderTTaskRedisConnection[Int] =
    send(DecrByRequest(UUID.randomUUID(), key, value)).flatMap {
      case DecrBySucceeded(_, _, value) => ReaderTTask.pure(value)
      case DecrByFailed(_, _, ex)       => ReaderTTask.raiseError(ex)
    }

  def get(key: String): ReaderTTaskRedisConnection[Option[String]] =
    send(GetRequest(UUID.randomUUID(), key)).flatMap {
      case GetSucceeded(_, _, value) => ReaderTTask.pure(value)
      case GetFailed(_, _, ex)       => ReaderTTask.raiseError(ex)
    }

  def getBit(key: String, offset: Int): ReaderTTaskRedisConnection[Int] =
    send(GetBitRequest(UUID.randomUUID(), key, offset)).flatMap {
      case GetBitSucceeded(_, _, value) => ReaderTTask.pure(value)
      case GetBitFailed(_, _, ex)       => ReaderTTask.raiseError(ex)
    }

  def getRange(key: String, startAndEnd: StartAndEnd): ReaderTTaskRedisConnection[Option[String]] =
    send(GetRangeRequest(UUID.randomUUID(), key, startAndEnd)).flatMap {
      case GetRangeSucceeded(_, _, value) => ReaderTTask.pure(value)
      case GetRangeFailed(_, _, ex)       => ReaderTTask.raiseError(ex)
    }

  def getSet(key: String, value: String): ReaderTTaskRedisConnection[Option[String]] =
    send(GetSetRequest(UUID.randomUUID(), key, value)).flatMap {
      case GetSetSucceeded(_, _, result) => ReaderTTask.pure(result)
      case GetSetFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def incr(key: String): ReaderTTaskRedisConnection[Int] = send(IncrRequest(UUID.randomUUID(), key)).flatMap {
    case IncrSucceeded(_, _, value) => ReaderTTask.pure(value)
    case IncrFailed(_, _, ex)       => ReaderTTask.raiseError(ex)
  }

  def incrBy(key: String, value: Int): ReaderTTaskRedisConnection[Int] =
    send(IncrByRequest(UUID.randomUUID(), key, value)).flatMap {
      case IncrBySucceeded(_, _, value) => ReaderTTask.pure(value)
      case IncrByFailed(_, _, ex)       => ReaderTTask.raiseError(ex)
    }

  def incrByFloat(key: String, value: Double): ReaderTTaskRedisConnection[Double] =
    send(IncrByFloatRequest(UUID.randomUUID(), key, value)).flatMap {
      case IncrByFloatSucceeded(_, _, value) => ReaderTTask.pure(value)
      case IncrByFloatFailed(_, _, ex)       => ReaderTTask.raiseError(ex)
    }

  def set[A](key: String, value: A)(implicit TS: ToString[A]): ReaderTTaskRedisConnection[Unit] =
    send(SetRequest(UUID.randomUUID(), key, value)).flatMap {
      case SetSucceeded(_, _)  => ReaderTTask.pure(())
      case SetFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

}
