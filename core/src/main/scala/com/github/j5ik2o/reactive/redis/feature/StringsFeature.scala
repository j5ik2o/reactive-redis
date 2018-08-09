package com.github.j5ik2o.reactive.redis.feature

import java.util.UUID

import cats.Show
import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis._
import com.github.j5ik2o.reactive.redis.command.strings._

import scala.concurrent.duration.FiniteDuration

/**
  * https://redis.io/commands#string
  */
trait StringsAPI[M[_]] {
  def append(key: String, value: String): M[Result[Long]]
  def bitCount(key: String, startAndEnd: Option[StartAndEnd] = None): M[Result[Long]]
  def bitField(key: String, options: BitFieldRequest.SubOption*): M[Result[Seq[Long]]]
  def bitOp(operand: BitOpRequest.Operand, outputKey: String, inputKey1: String, inputKey2: String): M[Result[Long]]
  def bitPos(key: String, bit: Int, startAndEnd: Option[BitPosRequest.StartAndEnd] = None): M[Result[Long]]
  def decr(key: String): M[Result[Long]]
  def decrBy(key: String, value: Int): M[Result[Long]]
  def get(key: String): M[Result[Option[String]]]
  def getBit(key: String, offset: Int): M[Result[Long]]
  def getRange(key: String, startAndEnd: StartAndEnd): M[Result[Option[String]]]
  def getSet(key: String, value: String): M[Result[Option[String]]]
  def incr(key: String): M[Result[Long]]
  def incrBy(key: String, value: Int): M[Result[Long]]
  def incrByFloat(key: String, value: Double): M[Result[Double]]
  def mGet(key: String, keys: String*): M[Result[Seq[String]]]
  def mGet(keys: NonEmptyList[String]): M[Result[Seq[String]]]
  def mSet(values: Map[String, Any]): M[Result[Unit]]
  def mSetNx(values: Map[String, Any]): M[Result[Boolean]]
  def pSetEx[A: Show](key: String, millis: FiniteDuration, value: A): M[Result[Unit]]
  def set[A: Show](key: String,
                   value: A,
                   expiration: Option[SetExpiration] = None,
                   setOption: Option[SetOption] = None): M[Result[Unit]]
  def setBit(key: String, offset: Int, value: Int): M[Result[Long]]
  def setEx[A: Show](key: String, expires: FiniteDuration, value: A): M[Result[Unit]]
  def setNx[A: Show](key: String, value: A): M[Result[Boolean]]
  def setRange[A: Show](key: String, range: Int, value: A): M[Result[Long]]
  def strLen(key: String): M[Result[Long]]
}

trait StringsFeature extends StringsAPI[ReaderTTaskRedisConnection] {
  this: RedisClient =>

  override def append(key: String, value: String): ReaderTTaskRedisConnection[Result[Long]] =
    send(AppendRequest(UUID.randomUUID(), key, value)).flatMap {
      case AppendSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case AppendSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case AppendFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def bitCount(key: String,
                        startAndEnd: Option[StartAndEnd] = None): ReaderTTaskRedisConnection[Result[Long]] =
    send(BitCountRequest(UUID.randomUUID(), key, startAndEnd)).flatMap {
      case BitCountSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case BitCountSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case BitCountFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def bitField(key: String,
                        options: BitFieldRequest.SubOption*): ReaderTTaskRedisConnection[Result[Seq[Long]]] =
    send(BitFieldRequest(UUID.randomUUID(), key, options: _*)).flatMap {
      case BitFieldSuspended(_, _)          => ReaderTTask.pure(Suspended)
      case BitFieldSucceeded(_, _, results) => ReaderTTask.pure(Provided(results))
      case BitFieldFailed(_, _, ex)         => ReaderTTask.raiseError(ex)
    }

  override def bitOp(operand: BitOpRequest.Operand,
                     outputKey: String,
                     inputKey1: String,
                     inputKey2: String): ReaderTTaskRedisConnection[Result[Long]] =
    send(BitOpRequest(UUID.randomUUID(), operand, outputKey, inputKey1, inputKey2)).flatMap {
      case BitOpSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case BitOpSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case BitOpFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def bitPos(key: String,
                      bit: Int,
                      startAndEnd: Option[BitPosRequest.StartAndEnd] = None): ReaderTTaskRedisConnection[Result[Long]] =
    send(BitPosRequest(UUID.randomUUID(), key, bit, startAndEnd)).flatMap {
      case BitPosSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case BitPosSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case BitPosFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def decr(key: String): ReaderTTaskRedisConnection[Result[Long]] =
    send(DecrRequest(UUID.randomUUID(), key)).flatMap {
      case DecrSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case DecrSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case DecrFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def decrBy(key: String, value: Int): ReaderTTaskRedisConnection[Result[Long]] =
    send(DecrByRequest(UUID.randomUUID(), key, value)).flatMap {
      case DecrBySuspended(_, _)         => ReaderTTask.pure(Suspended)
      case DecrBySucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case DecrByFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def get(key: String): ReaderTTaskRedisConnection[Result[Option[String]]] =
    send(GetRequest(UUID.randomUUID(), key)).flatMap {
      case GetSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case GetSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case GetFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def getBit(key: String, offset: Int): ReaderTTaskRedisConnection[Result[Long]] =
    send(GetBitRequest(UUID.randomUUID(), key, offset)).flatMap {
      case GetBitSuspended(_, _)        => ReaderTTask.pure(Suspended)
      case GetBitSucceeded(_, _, value) => ReaderTTask.pure(Provided(value))
      case GetBitFailed(_, _, ex)       => ReaderTTask.raiseError(ex)
    }

  override def getRange(key: String, startAndEnd: StartAndEnd): ReaderTTaskRedisConnection[Result[Option[String]]] =
    send(GetRangeRequest(UUID.randomUUID(), key, startAndEnd)).flatMap {
      case GetRangeSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case GetRangeSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case GetRangeFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def getSet(key: String, value: String): ReaderTTaskRedisConnection[Result[Option[String]]] =
    send(GetSetRequest(UUID.randomUUID(), key, value)).flatMap {
      case GetSetSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case GetSetSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case GetSetFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def incr(key: String): ReaderTTaskRedisConnection[Result[Long]] =
    send(IncrRequest(UUID.randomUUID(), key)).flatMap {
      case IncrSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case IncrSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case IncrFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def incrBy(key: String, value: Int): ReaderTTaskRedisConnection[Result[Long]] =
    send(IncrByRequest(UUID.randomUUID(), key, value)).flatMap {
      case IncrBySuspended(_, _)         => ReaderTTask.pure(Suspended)
      case IncrBySucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case IncrByFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def incrByFloat(key: String, value: Double): ReaderTTaskRedisConnection[Result[Double]] =
    send(IncrByFloatRequest(UUID.randomUUID(), key, value)).flatMap {
      case IncrByFloatSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case IncrByFloatSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case IncrByFloatFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def mGet(key: String, keys: String*): ReaderTTaskRedisConnection[Result[Seq[String]]] =
    mGet(NonEmptyList.of(key, keys: _*))

  override def mGet(keys: NonEmptyList[String]): ReaderTTaskRedisConnection[Result[Seq[String]]] =
    send(MGetRequest(UUID.randomUUID(), keys)).flatMap {
      case MGetSuspended(_, _)          => ReaderTTask.pure(Suspended)
      case MGetSucceeded(_, _, results) => ReaderTTask.pure(Provided(results))
      case MGetFailed(_, _, ex)         => ReaderTTask.raiseError(ex)
    }

  override def mSet(values: Map[String, Any]): ReaderTTaskRedisConnection[Result[Unit]] =
    send(MSetRequest(UUID.randomUUID(), values)).flatMap {
      case MSetSuspended(_, _)  => ReaderTTask.pure(Suspended)
      case MSetSucceeded(_, _)  => ReaderTTask.pure(Provided(()))
      case MSetFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

  override def mSetNx(values: Map[String, Any]): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(MSetNxRequest(UUID.randomUUID(), values)).flatMap {
      case MSetNxSuspended(_, _)    => ReaderTTask.pure(Suspended)
      case MSetNxSucceeded(_, _, n) => ReaderTTask.pure(Provided(n))
      case MSetNxFailed(_, _, ex)   => ReaderTTask.raiseError(ex)
    }

  override def pSetEx[A: Show](key: String,
                               millis: FiniteDuration,
                               value: A): ReaderTTaskRedisConnection[Result[Unit]] =
    send(PSetExRequest(UUID.randomUUID(), key, millis, value)).flatMap {
      case PSetExSuspended(_, _)  => ReaderTTask.pure(Suspended)
      case PSetExSucceeded(_, _)  => ReaderTTask.pure(Provided(()))
      case PSetExFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

  override def set[A: Show](key: String,
                            value: A,
                            expiration: Option[SetExpiration],
                            setOption: Option[SetOption]): ReaderTTaskRedisConnection[Result[Unit]] =
    send(SetRequest(UUID.randomUUID(), key, value, expiration, setOption)).flatMap {
      case SetSuspended(_, _)  => ReaderTTask.pure(Suspended)
      case SetSucceeded(_, _)  => ReaderTTask.pure(Provided(()))
      case SetFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

  override def setBit(key: String, offset: Int, value: Int): ReaderTTaskRedisConnection[Result[Long]] =
    send(SetBitRequest(UUID.randomUUID(), key, offset, value)).flatMap {
      case SetBitSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case SetBitSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case SetBitFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def setEx[A: Show](key: String,
                              expires: FiniteDuration,
                              value: A): ReaderTTaskRedisConnection[Result[Unit]] =
    send(SetExRequest(UUID.randomUUID(), key, expires, value)).flatMap {
      case SetExSuspended(_, _)  => ReaderTTask.pure(Suspended)
      case SetExSucceeded(_, _)  => ReaderTTask.pure(Provided(()))
      case SetExFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

  override def setNx[A: Show](key: String, value: A): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(SetNxRequest(UUID.randomUUID(), key, value)).flatMap {
      case SetNxSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case SetNxSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case SetNxFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def setRange[A: Show](key: String, range: Int, value: A): ReaderTTaskRedisConnection[Result[Long]] =
    send(SetRangeRequest(UUID.randomUUID(), key, range, value)).flatMap {
      case SetRangeSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case SetRangeSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case SetRangeFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def strLen(key: String): ReaderTTaskRedisConnection[Result[Long]] =
    send(StrLenRequest(UUID.randomUUID(), key)).flatMap {
      case StrLenSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case StrLenSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case StrLenFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

}
