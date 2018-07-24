package com.github.j5ik2o.reactive.redis.feature

import java.util.UUID

import cats.Show
import com.github.j5ik2o.reactive.redis._
import com.github.j5ik2o.reactive.redis.command.strings._

import scala.concurrent.duration.FiniteDuration

/**
  * https://redis.io/commands#string
  */
trait StringsFeature {
  this: RedisClient =>

  def append(key: String, value: String): ReaderTTaskRedisConnection[Result[Int]] =
    send(AppendRequest(UUID.randomUUID(), key, value)).flatMap {
      case AppendSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case AppendSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case AppendFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def bitCount(key: String, startAndEnd: Option[StartAndEnd] = None): ReaderTTaskRedisConnection[Result[Int]] =
    send(BitCountRequest(UUID.randomUUID(), key, startAndEnd)).flatMap {
      case BitCountSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case BitCountSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case BitCountFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def bitField(key: String, options: BitFieldRequest.SubOption*): ReaderTTaskRedisConnection[Result[Seq[Int]]] =
    send(BitFieldRequest(UUID.randomUUID(), key, options: _*)).flatMap {
      case BitFieldSuspended(_, _)          => ReaderTTask.pure(Suspended)
      case BitFieldSucceeded(_, _, results) => ReaderTTask.pure(Provided(results))
      case BitFieldFailed(_, _, ex)         => ReaderTTask.raiseError(ex)
    }

  def bitOp(operand: BitOpRequest.Operand,
            outputKey: String,
            inputKey1: String,
            inputKey2: String): ReaderTTaskRedisConnection[Result[Int]] =
    send(BitOpRequest(UUID.randomUUID(), operand, outputKey, inputKey1, inputKey2)).flatMap {
      case BitOpSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case BitOpSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case BitOpFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def bitPos(key: String,
             bit: Int,
             startAndEnd: Option[BitPosRequest.StartAndEnd] = None): ReaderTTaskRedisConnection[Result[Int]] =
    send(BitPosRequest(UUID.randomUUID(), key, bit, startAndEnd)).flatMap {
      case BitPosSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case BitPosSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case BitPosFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def decr(key: String): ReaderTTaskRedisConnection[Result[Int]] = send(DecrRequest(UUID.randomUUID(), key)).flatMap {
    case DecrSuspended(_, _)         => ReaderTTask.pure(Suspended)
    case DecrSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
    case DecrFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
  }

  def decrBy(key: String, value: Int): ReaderTTaskRedisConnection[Result[Int]] =
    send(DecrByRequest(UUID.randomUUID(), key, value)).flatMap {
      case DecrBySuspended(_, _)         => ReaderTTask.pure(Suspended)
      case DecrBySucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case DecrByFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def get(key: String): ReaderTTaskRedisConnection[Result[Option[String]]] =
    send(GetRequest(UUID.randomUUID(), key)).flatMap {
      case GetSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case GetSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case GetFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def getBit(key: String, offset: Int): ReaderTTaskRedisConnection[Result[Int]] =
    send(GetBitRequest(UUID.randomUUID(), key, offset)).flatMap {
      case GetBitSuspended(_, _)        => ReaderTTask.pure(Suspended)
      case GetBitSucceeded(_, _, value) => ReaderTTask.pure(Provided(value))
      case GetBitFailed(_, _, ex)       => ReaderTTask.raiseError(ex)
    }

  def getRange(key: String, startAndEnd: StartAndEnd): ReaderTTaskRedisConnection[Result[Option[String]]] =
    send(GetRangeRequest(UUID.randomUUID(), key, startAndEnd)).flatMap {
      case GetRangeSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case GetRangeSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case GetRangeFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def getSet(key: String, value: String): ReaderTTaskRedisConnection[Result[Option[String]]] =
    send(GetSetRequest(UUID.randomUUID(), key, value)).flatMap {
      case GetSetSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case GetSetSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case GetSetFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def incr(key: String): ReaderTTaskRedisConnection[Result[Int]] = send(IncrRequest(UUID.randomUUID(), key)).flatMap {
    case IncrSuspended(_, _)         => ReaderTTask.pure(Suspended)
    case IncrSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
    case IncrFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
  }

  def incrBy(key: String, value: Int): ReaderTTaskRedisConnection[Result[Int]] =
    send(IncrByRequest(UUID.randomUUID(), key, value)).flatMap {
      case IncrBySuspended(_, _)         => ReaderTTask.pure(Suspended)
      case IncrBySucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case IncrByFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def incrByFloat(key: String, value: Double): ReaderTTaskRedisConnection[Result[Double]] =
    send(IncrByFloatRequest(UUID.randomUUID(), key, value)).flatMap {
      case IncrByFloatSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case IncrByFloatSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case IncrByFloatFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def mGet(keys: Seq[String]): ReaderTTaskRedisConnection[Result[Seq[String]]] =
    send(MGetRequest(UUID.randomUUID(), keys)).flatMap {
      case MGetSuspended(_, _)          => ReaderTTask.pure(Suspended)
      case MGetSucceeded(_, _, results) => ReaderTTask.pure(Provided(results))
      case MGetFailed(_, _, ex)         => ReaderTTask.raiseError(ex)
    }

  def mSet(values: Map[String, Any]): ReaderTTaskRedisConnection[Result[Unit]] =
    send(MSetRequest(UUID.randomUUID(), values)).flatMap {
      case MSetSuspended(_, _)  => ReaderTTask.pure(Suspended)
      case MSetSucceeded(_, _)  => ReaderTTask.pure(Provided(()))
      case MSetFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

  def mSetNx(values: Map[String, Any]): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(MSetNxRequest(UUID.randomUUID(), values)).flatMap {
      case MSetNxSuspended(_, _)    => ReaderTTask.pure(Suspended)
      case MSetNxSucceeded(_, _, n) => ReaderTTask.pure(Provided(n))
      case MSetNxFailed(_, _, ex)   => ReaderTTask.raiseError(ex)
    }

  def pSetEx[A: Show](key: String, millis: FiniteDuration, value: A): ReaderTTaskRedisConnection[Result[Unit]] =
    send(PSetExRequest(UUID.randomUUID(), key, millis, value)).flatMap {
      case PSetExSuspended(_, _)  => ReaderTTask.pure(Suspended)
      case PSetExSucceeded(_, _)  => ReaderTTask.pure(Provided(()))
      case PSetExFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

  def set[A: Show](key: String, value: A): ReaderTTaskRedisConnection[Result[Unit]] =
    send(SetRequest(UUID.randomUUID(), key, value)).flatMap {
      case SetSuspended(_, _)  => ReaderTTask.pure(Suspended)
      case SetSucceeded(_, _)  => ReaderTTask.pure(Provided(()))
      case SetFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

  def setBit(key: String, offset: Int, value: Int): ReaderTTaskRedisConnection[Result[Int]] =
    send(SetBitRequest(UUID.randomUUID(), key, offset, value)).flatMap {
      case SetBitSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case SetBitSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case SetBitFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def setEx[A: Show](key: String, expires: FiniteDuration, value: A): ReaderTTaskRedisConnection[Result[Unit]] =
    send(SetExRequest(UUID.randomUUID(), key, expires, value)).flatMap {
      case SetExSuspended(_, _)  => ReaderTTask.pure(Suspended)
      case SetExSucceeded(_, _)  => ReaderTTask.pure(Provided(()))
      case SetExFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

  def setRange[A: Show](key: String, range: Int, value: A): ReaderTTaskRedisConnection[Result[Int]] =
    send(SetRangeRequest(UUID.randomUUID(), key, range, value)).flatMap {
      case SetRangeSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case SetRangeSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case SetRangeFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def strLen(key: String): ReaderTTaskRedisConnection[Result[Int]] =
    send(StrLenRequest(UUID.randomUUID(), key)).flatMap {
      case StrLenSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case StrLenSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case StrLenFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

}
