package com.github.j5ik2o.reactive.redis

import java.time.ZonedDateTime
import java.util.UUID

import akka.actor.ActorSystem
import cats.Show
import cats.data.{ NonEmptyList, ReaderT }
import com.github.j5ik2o.reactive.redis.command._
import com.github.j5ik2o.reactive.redis.command.connection.{ PingFailed, PingRequest, PingSucceeded }
import com.github.j5ik2o.reactive.redis.command.keys._
import com.github.j5ik2o.reactive.redis.command.strings._
import monix.eval.Task

import scala.concurrent.duration.FiniteDuration

object RedisClient {

  def apply()(implicit system: ActorSystem): RedisClient = new RedisClient()

}

trait ConnectionClient { this: RedisClient =>

  def ping(message: Option[String] = None): ReaderTTaskRedisConnection[String] =
    send(PingRequest(UUID.randomUUID(), message)).flatMap {
      case PingSucceeded(_, _, result) => ReaderTTask.pure(result)
      case PingFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

}

trait KeysClient { this: RedisClient =>

  def del(keys: NonEmptyList[String]): ReaderTTaskRedisConnection[Int] =
    send(DelRequest(UUID.randomUUID(), keys)).flatMap {
      case DelSucceeded(_, _, result) => ReaderTTask.pure(result)
      case DelFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def dump(key: String): ReaderTTaskRedisConnection[Option[Array[Byte]]] =
    send(DumpRequest(UUID.randomUUID(), key)).flatMap {
      case DumpSucceeded(_, _, result) => ReaderTTask.pure(result)
      case DumpFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def exists(keys: NonEmptyList[String]): ReaderTTaskRedisConnection[Boolean] =
    send(ExistsRequest(UUID.randomUUID(), keys)).flatMap {
      case ExistsSucceeded(_, _, result) => ReaderTTask.pure(result)
      case ExistsFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def expire(key: String, seconds: FiniteDuration): ReaderTTaskRedisConnection[Boolean] =
    send(ExpireRequest(UUID.randomUUID(), key, seconds)).flatMap {
      case ExpireSucceeded(_, _, result) => ReaderTTask.pure(result)
      case ExpireFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def expireAt(key: String, expiresAt: ZonedDateTime): ReaderTTaskRedisConnection[Boolean] =
    send(ExpireAtRequest(UUID.randomUUID(), key, expiresAt)).flatMap {
      case ExpireAtSucceeded(_, _, result) => ReaderTTask.pure(result)
      case ExpireAtFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def keys(pattern: String): ReaderTTaskRedisConnection[Seq[Option[String]]] =
    send(KeysRequest(UUID.randomUUID(), pattern)).flatMap {
      case KeysSucceeded(_, _, results) => ReaderTTask.pure(results)
      case KeysFailed(_, _, ex)         => ReaderTTask.raiseError(ex)
    }

  def migrate(host: String,
              port: Int,
              key: String,
              toDbNo: Int,
              timeout: FiniteDuration): ReaderTTaskRedisConnection[Unit] =
    send(MigrateRequest(UUID.randomUUID(), host, port, key, toDbNo, timeout)).flatMap {
      case MigrateSucceeded(_, _)  => ReaderTTask.pure(())
      case MigrateFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

  def move(key: String, db: Int): ReaderTTaskRedisConnection[Boolean] =
    send(MoveRequest(UUID.randomUUID(), key, db)).flatMap {
      case MoveSucceeded(_, _, result) => ReaderTTask.pure(result)
      case MoveFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  // object

  def persist(key: String): ReaderTTaskRedisConnection[Boolean] = send(PersistRequest(UUID.randomUUID(), key)).flatMap {
    case PersistSucceeded(_, _, result) => ReaderTTask.pure(result)
    case PersistFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
  }

  def pExpire(key: String, milliseconds: FiniteDuration): ReaderTTaskRedisConnection[Boolean] =
    send(PExpireRequest(UUID.randomUUID(), key, milliseconds)).flatMap {
      case PExpireSucceeded(_, _, result) => ReaderTTask.pure(result)
      case PExpireFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def pExpireAt = ???

  /**
  * PEXPIREAT
  * PTTL
  * RANDOMKEY
  * RENAME
  * RENAMENX
  * RESTORE
  * SCAN
  * SORT
  * TOUCH
  * TTL
  * TYPE
  * UNLINK
  * WAIT
  */
}

trait StringsClient { this: RedisClient =>

  def append(key: String, value: String): ReaderTTaskRedisConnection[Int] =
    send(AppendRequest(UUID.randomUUID(), key, value)).flatMap {
      case AppendSucceeded(_, _, result) => ReaderTTask.pure(result)
      case AppendFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def bitCount(key: String, startAndEnd: Option[StartAndEnd]): ReaderTTaskRedisConnection[Int] =
    send(BitCountRequest(UUID.randomUUID(), key, startAndEnd)).flatMap {
      case BitCountSucceeded(_, _, result) => ReaderTTask.pure(result)
      case BitCountFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def bitField(key: String, options: BitFieldRequest.SubOption*): ReaderTTaskRedisConnection[Seq[Int]] =
    send(BitFieldRequest(UUID.randomUUID(), key, options: _*)).flatMap {
      case BitFieldSucceeded(_, _, results) => ReaderTTask.pure(results)
      case BitFieldFailed(_, _, ex)         => ReaderTTask.raiseError(ex)
    }

  def bitOp(operand: BitOpRequest.Operand,
            outputKey: String,
            inputKey1: String,
            inputKey2: String): ReaderTTaskRedisConnection[Int] =
    send(BitOpRequest(UUID.randomUUID(), operand, outputKey, inputKey1, inputKey2)).flatMap {
      case BitOpSucceeded(_, _, result) => ReaderTTask.pure(result)
      case BitOpFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def bitPos(key: String, bit: Int, startAndEnd: Option[StartAndEnd] = None): ReaderTTaskRedisConnection[Int] =
    send(BitPosRequest(UUID.randomUUID(), key, bit, startAndEnd)).flatMap {
      case BitPosSucceeded(_, _, result) => ReaderTTask.pure(result)
      case BitPosFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def decr(key: String): ReaderTTaskRedisConnection[Int] = send(DecrRequest(UUID.randomUUID(), key)).flatMap {
    case DecrSucceeded(_, _, result) => ReaderTTask.pure(result)
    case DecrFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
  }

  def decrBy(key: String, value: Int): ReaderTTaskRedisConnection[Int] =
    send(DecrByRequest(UUID.randomUUID(), key, value)).flatMap {
      case DecrBySucceeded(_, _, result) => ReaderTTask.pure(result)
      case DecrByFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def get(key: String): ReaderTTaskRedisConnection[Option[String]] =
    send(GetRequest(UUID.randomUUID(), key)).flatMap {
      case GetSuspended(_, _)         => ReaderTTask.pure(None)
      case GetSucceeded(_, _, result) => ReaderTTask.pure(result)
      case GetFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def getBit(key: String, offset: Int): ReaderTTaskRedisConnection[Int] =
    send(GetBitRequest(UUID.randomUUID(), key, offset)).flatMap {
      case GetBitSucceeded(_, _, value) => ReaderTTask.pure(value)
      case GetBitFailed(_, _, ex)       => ReaderTTask.raiseError(ex)
    }

  def getRange(key: String, startAndEnd: StartAndEnd): ReaderTTaskRedisConnection[Option[String]] =
    send(GetRangeRequest(UUID.randomUUID(), key, startAndEnd)).flatMap {
      case GetRangeSucceeded(_, _, result) => ReaderTTask.pure(result)
      case GetRangeFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def getSet(key: String, value: String): ReaderTTaskRedisConnection[Option[String]] =
    send(GetSetRequest(UUID.randomUUID(), key, value)).flatMap {
      case GetSetSucceeded(_, _, result) => ReaderTTask.pure(result)
      case GetSetFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def incr(key: String): ReaderTTaskRedisConnection[Int] = send(IncrRequest(UUID.randomUUID(), key)).flatMap {
    case IncrSucceeded(_, _, result) => ReaderTTask.pure(result)
    case IncrFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
  }

  def incrBy(key: String, value: Int): ReaderTTaskRedisConnection[Int] =
    send(IncrByRequest(UUID.randomUUID(), key, value)).flatMap {
      case IncrBySucceeded(_, _, result) => ReaderTTask.pure(result)
      case IncrByFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def incrByFloat(key: String, value: Double): ReaderTTaskRedisConnection[Double] =
    send(IncrByFloatRequest(UUID.randomUUID(), key, value)).flatMap {
      case IncrByFloatSucceeded(_, _, result) => ReaderTTask.pure(result)
      case IncrByFloatFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def mGet(keys: Seq[String]): ReaderTTaskRedisConnection[Seq[Option[String]]] =
    send(MGetRequest(UUID.randomUUID(), keys)).flatMap {
      case MGetSucceeded(_, _, results) => ReaderTTask.pure(results)
      case MGetFailed(_, _, ex)         => ReaderTTask.raiseError(ex)
    }

  def mSet(values: Map[String, Any]): ReaderTTaskRedisConnection[Unit] =
    send(MSetRequest(UUID.randomUUID(), values)).flatMap {
      case MSetSucceeded(_, _)  => ReaderTTask.pure(())
      case MSetFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

  def mSetNx(values: Map[String, Any]): ReaderTTaskRedisConnection[Boolean] =
    send(MSetNxRequest(UUID.randomUUID(), values)).flatMap {
      case MSetNxSucceeded(_, _, n) => ReaderTTask.pure(n)
      case MSetNxFailed(_, _, ex)   => ReaderTTask.raiseError(ex)
    }

  def pSetEx[A: Show](key: String, millis: FiniteDuration, value: A): ReaderTTaskRedisConnection[Unit] =
    send(PSetExRequest(UUID.randomUUID(), key, millis, value)).flatMap {
      case PSetExSucceeded(_, _)  => ReaderTTask.pure(())
      case PSetExFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

  def set[A: Show](key: String, value: A): ReaderTTaskRedisConnection[Unit] =
    send(SetRequest(UUID.randomUUID(), key, value)).flatMap {
      case SetSuspended(_, _)  => ReaderTTask.pure(())
      case SetSucceeded(_, _)  => ReaderTTask.pure(())
      case SetFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

  def setBit(key: String, offset: Int, value: Int): ReaderTTaskRedisConnection[Int] =
    send(SetBitRequest(UUID.randomUUID(), key, offset, value)).flatMap {
      case SetBitSucceeded(_, _, result) => ReaderTTask.pure(result)
      case SetBitFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def setEx[A: Show](key: String, expires: FiniteDuration, value: A): ReaderTTaskRedisConnection[Unit] =
    send(SetExRequest(UUID.randomUUID(), key, expires, value)).flatMap {
      case SetExSucceeded(_, _)  => ReaderTTask.pure(())
      case SetExFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

  def setRange[A: Show](key: String, range: Int, value: A): ReaderTTaskRedisConnection[Int] =
    send(SetRangeRequest(UUID.randomUUID(), key, range, value)).flatMap {
      case SetRangeSucceeded(_, _, result) => ReaderTTask.pure(result)
      case SetRangeFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def strLen(key: String): ReaderTTaskRedisConnection[Int] = send(StrLenRequest(UUID.randomUUID(), key)).flatMap {
    case StrLenSucceeded(_, _, result) => ReaderTTask.pure(result)
    case StrLenFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
  }

}

class RedisClient(implicit system: ActorSystem) extends StringsClient with KeysClient with ConnectionClient {

  def send[C <: CommandRequest](cmd: C): ReaderTTaskRedisConnection[cmd.Response] = ReaderT(_.send(cmd))

}
