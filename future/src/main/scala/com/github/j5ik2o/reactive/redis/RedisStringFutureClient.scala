package com.github.j5ik2o.reactive.redis

import java.util.UUID

import akka.pattern._
import com.github.j5ik2o.reactive.redis.Options.StartAndEnd
import com.github.j5ik2o.reactive.redis.StringsOperations._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future }

trait RedisStringFutureClient { this: RedisFutureClient =>

  private implicit val to = timeout

  // --- APPEND
  def append(key: String, value: String)(implicit ec: ExecutionContext): Future[Option[Int]] = {
    (redisActor ? AppendRequest(UUID.randomUUID(), key, value)).mapTo[AppendResponse].flatMap {
      case AppendFailed(_, _, ex)        => Future.failed(ex)
      case AppendSuspended(_, _)         => Future.successful(None)
      case AppendSucceeded(_, _, result) => Future.successful(Some(result))
    }
  }
  // --- BITCOUNT
  def bitCount(key: String,
               startAndEnd: Option[StartAndEnd] = None)(implicit ec: ExecutionContext): Future[Option[Int]] = {
    (redisActor ? BitCountRequest(UUID.randomUUID(), key, startAndEnd))
      .mapTo[BitCountResponse]
      .flatMap {
        case BitCountFailed(_, _, ex)        => Future.failed(ex)
        case BitCountSuspended(_, _)         => Future.successful(None)
        case BitCountSucceeded(_, _, result) => Future.successful(Some(result))
      }
  }
  // --- BITFIELD
  def bitField(key: String,
               options: BitFieldRequest.SubOption*)(implicit ec: ExecutionContext): Future[Option[Seq[Int]]] = {
    (redisActor ? BitFieldRequest(UUID.randomUUID(), key, options: _*))
      .mapTo[BitFieldResponse]
      .flatMap {
        case BitFieldFailed(_, _, ex)        => Future.failed(ex)
        case BitFieldSuspended(_, _)         => Future.successful(None)
        case BitFieldSucceeded(_, _, result) => Future.successful(Some(result))
      }
  }
  // --- BITOP
  def bitOp(operand: BitOpRequest.Operand, outputKey: String, inputKey1: String, inputKey2: String)(
      implicit ec: ExecutionContext
  ): Future[Option[Int]] = {
    (redisActor ? BitOpRequest(UUID.randomUUID(), operand, outputKey, inputKey1, inputKey2))
      .mapTo[BitOpResponse]
      .flatMap {
        case BitOpFailed(_, _, ex)        => Future.failed(ex)
        case BitOpSuspended(_, _)         => Future.successful(None)
        case BitOpSucceeded(_, _, result) => Future.successful(Some(result))
      }
  }
  // --- BITPOS
  def bitPos(key: String, bit: Int, startAndEnd: Option[StartAndEnd] = None)(
      implicit ec: ExecutionContext
  ): Future[Option[Int]] = {
    (redisActor ? BitPosRequest(UUID.randomUUID(), key, bit, startAndEnd))
      .mapTo[BitPosResponse]
      .flatMap {
        case BitPosFailed(_, _, ex)        => Future.failed(ex)
        case BitPosSuspended(_, _)         => Future.successful(None)
        case BitPosSucceeded(_, _, result) => Future.successful(Some(result))
      }
  }
  // --- DECR
  def descr(key: String)(implicit ec: ExecutionContext): Future[Option[Int]] = {
    (redisActor ? DecrRequest(UUID.randomUUID(), key)).mapTo[DecrResponse].flatMap {
      case DecrFailed(_, _, ex)       => Future.failed(ex)
      case DecrSuspended(_, _)        => Future.successful(None)
      case DecrSucceeded(_, _, value) => Future.successful(Some(value))
    }
  }
  // --- DECRBY
  def descrBy(key: String, value: Int)(implicit ec: ExecutionContext): Future[Option[Int]] = {
    (redisActor ? DecrByRequest(UUID.randomUUID(), key, value)).mapTo[DecrByResponse].flatMap {
      case DecrByFailed(_, _, ex)        => Future.failed(ex)
      case DecrBySuspended(_, _)         => Future.successful(None)
      case DecrBySucceeded(_, _, result) => Future.successful(Some(result))
    }
  }
  // --- GET
  def get(key: String)(implicit ec: ExecutionContext): Future[Option[String]] = {
    (redisActor ? GetRequest(UUID.randomUUID(), key)).mapTo[GetResponse].flatMap {
      case GetFailed(_, _, ex)       => Future.failed(ex)
      case GetSuspended(_, _)        => Future.successful(None)
      case GetSucceeded(_, _, value) => Future.successful(value)
    }
  }
  // --- GETBIT
  def getBit(key: String, offset: Int)(implicit ec: ExecutionContext): Future[Option[Int]] = {
    (redisActor ? GetBitRequest(UUID.randomUUID(), key, offset)).mapTo[GetBitResponse].flatMap {
      case GetBitFailed(_, _, ex)       => Future.failed(ex)
      case GetBitSuspended(_, _)        => Future.successful(None)
      case GetBitSucceeded(_, _, value) => Future.successful(Some(value))
    }
  }
  // --- GETRANGE
  def getRange(key: String, startAndEnd: StartAndEnd)(implicit ec: ExecutionContext): Future[Option[String]] = {
    (redisActor ? GetRangeRequest(UUID.randomUUID(), key, startAndEnd)).mapTo[GetRangeResponse].flatMap {
      case GetRangeFailed(_, _, ex)       => Future.failed(ex)
      case GetRangeSuspended(_, _)        => Future.successful(None)
      case GetRangeSucceeded(_, _, value) => Future.successful(value)
    }
  }
  // --- GETSET
  def getSet(key: String, value: String)(implicit ec: ExecutionContext): Future[Option[String]] = {
    (redisActor ? GetSetRequest(UUID.randomUUID(), key, value)).mapTo[GetSetResponse].flatMap {
      case GetSetFailed(_, _, ex)        => Future.failed(ex)
      case GetSetSuspended(_, _)         => Future.successful(None)
      case GetSetSucceeded(_, _, result) => Future.successful(result)
    }
  }
  // --- INCR
  def incr(key: String)(implicit ec: ExecutionContext): Future[Option[Int]] = {
    (redisActor ? IncrRequest(UUID.randomUUID(), key)).mapTo[IncrResponse].flatMap {
      case IncrFailed(_, _, ex)        => Future.failed(ex)
      case IncrSuspended(_, _)         => Future.successful(None)
      case IncrSucceeded(_, _, result) => Future.successful(Some(result))
    }
  }
  // --- INCRBY
  def incrBy(key: String, value: Int)(implicit ec: ExecutionContext): Future[Option[Int]] = {
    (redisActor ? IncrByRequest(UUID.randomUUID(), key, value)).mapTo[IncrByResponse].flatMap {
      case IncrByFailed(_, _, ex)        => Future.failed(ex)
      case IncrBySuspended(_, _)         => Future.successful(None)
      case IncrBySucceeded(_, _, result) => Future.successful(Some(result))
    }
  }
  // --- INCRBYFLOAT
  def incrByFloat(key: String, value: Double)(implicit ec: ExecutionContext): Future[Option[Double]] = {
    (redisActor ? IncrByFloatRequest(UUID.randomUUID(), key, value)).mapTo[IncrByFloatResponse].flatMap {
      case IncrByFloatFailed(_, _, ex)        => Future.failed(ex)
      case IncrByFloatSuspended(_, _)         => Future.successful(None)
      case IncrByFloatSucceeded(_, _, result) => Future.successful(Some(result))
    }
  }
  // --- MGET
  def mGet(keys: Seq[String])(implicit ec: ExecutionContext): Future[Option[Seq[Option[String]]]] = {
    (redisActor ? MGetRequest(UUID.randomUUID(), keys)).mapTo[MGetResponse].flatMap {
      case MGetFailed(_, _, ex)        => Future.failed(ex)
      case MGetSuspended(_, _)         => Future.successful(None)
      case MGetSucceeded(_, _, values) => Future.successful(Some(values))
    }
  }
  // --- MSET
  def mSet(values: Map[String, Any])(implicit ec: ExecutionContext): Future[Unit] = {
    (redisActor ? MSetRequest(UUID.randomUUID(), values)).mapTo[MSetResponse].flatMap {
      case MSetFailed(_, _, ex) => Future.failed(ex)
      case MSetSuspended(_, _)  => Future.successful(())
      case MSetSucceeded(_, _)  => Future.successful(())
    }
  }
  // --- MSETNX
  def mSetNx(values: Map[String, Any])(implicit ec: ExecutionContext): Future[Option[Boolean]] = {
    (redisActor ? MSetNxRequest(UUID.randomUUID(), values)).mapTo[MSetNxResponse].flatMap {
      case MSetNxFailed(_, _, ex)        => Future.failed(ex)
      case MSetNxSuspended(_, _)         => Future.successful(None)
      case MSetNxSucceeded(_, _, result) => Future.successful(Some(result))
    }
  }
  // --- PSETEX
  def pSetEx(key: String, duration: FiniteDuration, value: String)(implicit ec: ExecutionContext): Future[Unit] = {
    (redisActor ? PSetExRequest(UUID.randomUUID(), key, duration, value)).mapTo[PSetExResponse].flatMap {
      case PSetExFailed(_, _, ex) => Future.failed(ex)
      case PSetExSuspended(_, _)  => Future.successful(())
      case PSetExSucceeded(_, _)  => Future.successful(())
    }
  }
  // --- SET
  def set(key: String, value: String)(implicit ec: ExecutionContext): Future[Unit] = {
    (redisActor ? SetRequest(UUID.randomUUID(), key, value)).mapTo[SetResponse].flatMap {
      case SetFailed(_, _, ex) => Future.failed(ex)
      case SetSuspended(_, _)  => Future.successful(())
      case SetSucceeded(_, _)  => Future.successful(())
    }
  }
  // --- SETBIT
  def setBit(key: String, offset: Int, value: Int)(implicit ec: ExecutionContext): Future[Option[Int]] = {
    (redisActor ? SetBitRequest(UUID.randomUUID(), key, offset, value)).mapTo[SetBitResponse].flatMap {
      case SetBitFailed(_, _, ex)        => Future.failed(ex)
      case SetBitSuspended(_, _)         => Future.successful(None)
      case SetBitSucceeded(_, _, result) => Future.successful(Some(result))
    }
  }
  // --- SETEX
  def setEx(key: String, expires: FiniteDuration, value: String)(implicit ec: ExecutionContext): Future[Unit] = {
    (redisActor ? SetExRequest(UUID.randomUUID(), key, expires, value)).mapTo[SetExResponse].flatMap {
      case SetExFailed(_, _, ex) => Future.failed(ex)
      case SetExSuspended(_, _)  => Future.successful(())
      case SetExSucceeded(_, _)  => Future.successful(())
    }
  }
  // --- SETNX
  def setNx(key: String, value: String)(implicit ec: ExecutionContext): Future[Option[Boolean]] = {
    (redisActor ? SetNxRequest(UUID.randomUUID(), key, value)).mapTo[SetNxResponse].flatMap {
      case SetNxFailed(_, _, ex)        => Future.failed(ex)
      case SetNxSuspended(_, _)         => Future.successful(None)
      case SetNxSucceeded(_, _, result) => Future.successful(Some(result))
    }
  }
  // --- SETRANGE
  def setRange(key: String, range: Int, value: String)(implicit ec: ExecutionContext): Future[Option[Int]] = {
    (redisActor ? SetRangeRequest(UUID.randomUUID(), key, range, value)).mapTo[SetRangeResponse].flatMap {
      case SetRangeFailed(_, _, ex)        => Future.failed(ex)
      case SetRangeSuspended(_, _)         => Future.successful(None)
      case SetRangeSucceeded(_, _, result) => Future.successful(Some(result))
    }
  }
  // --- STRLEN
  def strlen(key: String)(implicit ec: ExecutionContext): Future[Option[Int]] = {
    (redisActor ? StrLenRequest(UUID.randomUUID(), key)).mapTo[StrLenResponse].flatMap {
      case StrLenFailed(_, _, ex)        => Future.failed(ex)
      case StrLenSuspended(_, _)         => Future.successful(None)
      case StrLenSucceeded(_, _, result) => Future.successful(Some(result))
    }
  }

}
