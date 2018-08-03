package com.github.j5ik2o.reactive.redis.feature

import java.time.ZonedDateTime
import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis._
import com.github.j5ik2o.reactive.redis.command.keys._

import scala.concurrent.duration.FiniteDuration

/**
  * https://redis.io/commands#generics
  */
trait KeysFeature {
  this: RedisClient =>

  def del(keys: NonEmptyList[String]): ReaderTTaskRedisConnection[Result[Long]] =
    send(DelRequest(UUID.randomUUID(), keys)).flatMap {
      case DelSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case DelSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case DelFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def dump(key: String): ReaderTTaskRedisConnection[Result[Option[Array[Byte]]]] =
    send(DumpRequest(UUID.randomUUID(), key)).flatMap {
      case DumpSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case DumpSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case DumpFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def exists(keys: NonEmptyList[String]): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(ExistsRequest(UUID.randomUUID(), keys)).flatMap {
      case ExistsSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case ExistsSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case ExistsFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def expire(key: String, seconds: FiniteDuration): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(ExpireRequest(UUID.randomUUID(), key, seconds)).flatMap {
      case ExpireSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case ExpireSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case ExpireFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def expireAt(key: String, expiresAt: ZonedDateTime): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(ExpireAtRequest(UUID.randomUUID(), key, expiresAt)).flatMap {
      case ExpireAtSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case ExpireAtSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case ExpireAtFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def keys(pattern: String): ReaderTTaskRedisConnection[Result[Seq[String]]] =
    send(KeysRequest(UUID.randomUUID(), pattern)).flatMap {
      case KeysSuspended(_, _)          => ReaderTTask.pure(Suspended)
      case KeysSucceeded(_, _, results) => ReaderTTask.pure(Provided(results))
      case KeysFailed(_, _, ex)         => ReaderTTask.raiseError(ex)
    }

  def migrate(host: String,
              port: Int,
              key: String,
              toDbNo: Int,
              timeout: FiniteDuration): ReaderTTaskRedisConnection[Result[Unit]] =
    send(MigrateRequest(UUID.randomUUID(), host, port, key, toDbNo, timeout)).flatMap {
      case MigrateSuspended(_, _)  => ReaderTTask.pure(Suspended)
      case MigrateSucceeded(_, _)  => ReaderTTask.pure(Provided(()))
      case MigrateFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

  def move(key: String, db: Int): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(MoveRequest(UUID.randomUUID(), key, db)).flatMap {
      case MoveSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case MoveSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case MoveFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  // object

  def persist(key: String): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(PersistRequest(UUID.randomUUID(), key)).flatMap {
      case PersistSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case PersistSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case PersistFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def pExpire(key: String, milliseconds: FiniteDuration): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(PExpireRequest(UUID.randomUUID(), key, milliseconds)).flatMap {
      case PExpireSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case PExpireSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case PExpireFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def pExpireAt(key: String, millisecondsTimestamp: ZonedDateTime): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(PExpireAtRequest(UUID.randomUUID(), key, millisecondsTimestamp)).flatMap {
      case PExpireAtSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case PExpireAtSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case PExpireAtFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  /**
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
