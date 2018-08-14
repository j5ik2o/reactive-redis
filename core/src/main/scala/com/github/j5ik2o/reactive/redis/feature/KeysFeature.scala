package com.github.j5ik2o.reactive.redis.feature

import java.time.ZonedDateTime
import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis._
import com.github.j5ik2o.reactive.redis.command.keys._

import scala.concurrent.duration.{ Duration, FiniteDuration }

/**
  * https://redis.io/commands#generics
  */
trait KeysAPI[M[_]] {
  def del(key: String, keys: String*): M[Result[Long]]
  def del(keys: NonEmptyList[String]): M[Result[Long]]
  def dump(key: String): M[Result[Option[Array[Byte]]]]
  def exists(key: String): M[Result[Boolean]]
  def expire(key: String, seconds: FiniteDuration): M[Result[Boolean]]
  def expireAt(key: String, expiresAt: ZonedDateTime): M[Result[Boolean]]
  def keys(pattern: String): M[Result[Seq[String]]]
  def migrate(host: String,
              port: Int,
              key: String,
              toDbNo: Int,
              timeout: FiniteDuration,
              copy: Boolean,
              replease: Boolean,
              keys: NonEmptyList[String]): M[Result[Status]]
  def move(key: String, db: Int): M[Result[Boolean]]
  def persist(key: String): M[Result[Boolean]]
  def pExpire(key: String, milliseconds: FiniteDuration): M[Result[Boolean]]
  def pExpireAt(key: String, millisecondsTimestamp: ZonedDateTime): M[Result[Boolean]]
  def pTtl(key: String): M[Result[Duration]]
  def randomKey(): M[Result[Option[String]]]
  def rename(key: String, newKey: String): M[Result[Unit]]
  def renameNx(key: String, newKey: String): M[Result[Boolean]]

  def waitReplicas(numOfReplicas: Int, timeout: Duration): M[Result[Long]]
}

trait KeysFeature extends KeysAPI[ReaderTTaskRedisConnection] {
  this: RedisClient =>

  override def del(key: String, keys: String*): ReaderTTaskRedisConnection[Result[Long]] =
    del(NonEmptyList.of(key, keys: _*))

  override def del(keys: NonEmptyList[String]): ReaderTTaskRedisConnection[Result[Long]] =
    send(DelRequest(UUID.randomUUID(), keys)).flatMap {
      case DelSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case DelSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case DelFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def dump(key: String): ReaderTTaskRedisConnection[Result[Option[Array[Byte]]]] =
    send(DumpRequest(UUID.randomUUID(), key)).flatMap {
      case DumpSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case DumpSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case DumpFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def exists(key: String): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(ExistsRequest(UUID.randomUUID(), key)).flatMap {
      case ExistsSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case ExistsSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case ExistsFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def expire(key: String, seconds: FiniteDuration): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(ExpireRequest(UUID.randomUUID(), key, seconds)).flatMap {
      case ExpireSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case ExpireSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case ExpireFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def expireAt(key: String, expiresAt: ZonedDateTime): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(ExpireAtRequest(UUID.randomUUID(), key, expiresAt)).flatMap {
      case ExpireAtSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case ExpireAtSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case ExpireAtFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def keys(pattern: String): ReaderTTaskRedisConnection[Result[Seq[String]]] =
    send(KeysRequest(UUID.randomUUID(), pattern)).flatMap {
      case KeysSuspended(_, _)          => ReaderTTask.pure(Suspended)
      case KeysSucceeded(_, _, results) => ReaderTTask.pure(Provided(results))
      case KeysFailed(_, _, ex)         => ReaderTTask.raiseError(ex)
    }

  override def migrate(host: String,
                       port: Int,
                       key: String,
                       toDbNo: Int,
                       timeout: FiniteDuration,
                       copy: Boolean,
                       replease: Boolean,
                       keys: NonEmptyList[String]): ReaderTTaskRedisConnection[Result[Status]] =
    send(MigrateRequest(UUID.randomUUID(), host, port, key, toDbNo, timeout, copy, replease, keys)).flatMap {
      case MigrateSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case MigrateSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case MigrateFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def move(key: String, db: Int): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(MoveRequest(UUID.randomUUID(), key, db)).flatMap {
      case MoveSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case MoveSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case MoveFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  // object

  override def persist(key: String): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(PersistRequest(UUID.randomUUID(), key)).flatMap {
      case PersistSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case PersistSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case PersistFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def pExpire(key: String, milliseconds: FiniteDuration): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(PExpireRequest(UUID.randomUUID(), key, milliseconds)).flatMap {
      case PExpireSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case PExpireSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case PExpireFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def pExpireAt(key: String,
                         millisecondsTimestamp: ZonedDateTime): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(PExpireAtRequest(UUID.randomUUID(), key, millisecondsTimestamp)).flatMap {
      case PExpireAtSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case PExpireAtSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case PExpireAtFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def pTtl(key: String): ReaderTTaskRedisConnection[Result[Duration]] =
    send(PTtlRequest(UUID.randomUUID(), key)).flatMap {
      case PTtlSuspended(_, _)             => ReaderTTask.pure(Suspended)
      case result @ PTtlSucceeded(_, _, _) => ReaderTTask.pure(Provided(result.toDuration))
      case PTtlFailed(_, _, ex)            => ReaderTTask.raiseError(ex)
    }

  override def randomKey(): ReaderTTaskRedisConnection[Result[Option[String]]] =
    send(RandomKeyRequest(UUID.randomUUID())).flatMap {
      case RandomKeySuspended(_, _)         => ReaderTTask.pure(Suspended)
      case RandomKeySucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case RandomKeyFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def rename(key: String, newKey: String): ReaderTTaskRedisConnection[Result[Unit]] =
    send(RenameRequest(UUID.randomUUID(), key, newKey)).flatMap {
      case RenameSuspended(_, _)  => ReaderTTask.pure(Suspended)
      case RenameSucceeded(_, _)  => ReaderTTask.pure(Provided(()))
      case RenameFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

  override def renameNx(key: String, newKey: String): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(RenameNxRequest(UUID.randomUUID(), key, newKey)).flatMap {
      case RenameNxSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case RenameNxSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case RenameNxFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  /**
    * RESTORE
    * SCAN
    * SORT
    * TOUCH
    * TTL
    * TYPE
    * UNLINK
    * WAIT
    */
  override def waitReplicas(numOfReplicas: Int, timeout: Duration): ReaderTTaskRedisConnection[Result[Long]] =
    send(WaitReplicasRequest(UUID.randomUUID(), numOfReplicas, timeout)).flatMap {
      case WaitReplicasSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case WaitReplicasSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case WaitReplicasFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }
}
