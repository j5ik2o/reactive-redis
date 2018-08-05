package com.github.j5ik2o.reactive.redis.feature

import java.util.UUID

import cats.Show
import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis._
import com.github.j5ik2o.reactive.redis.command.lists._

import scala.concurrent.duration.Duration

trait ListsAPI[M[_]] {
  def blpop(keys: NonEmptyList[String], timeout: Duration): M[Result[Seq[String]]]
  def brpop(keys: NonEmptyList[String], timeout: Duration): M[Result[Seq[String]]]
  def lpop(key: String): M[Result[Option[String]]]
  def lpush[A: Show](key: String, value: A): M[Result[Long]]
  def lpush[A: Show](key: String, values: NonEmptyList[A]): M[Result[Long]]
}

trait ListsFeature extends ListsAPI[ReaderTTaskRedisConnection] { this: RedisClient =>

  override def blpop(keys: NonEmptyList[String], timeout: Duration): ReaderTTaskRedisConnection[Result[Seq[String]]] =
    send(BLPopRequest(UUID.randomUUID(), keys, timeout)).flatMap {
      case BLPopSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case BLPopSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case BLPopFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def brpop(keys: NonEmptyList[String], timeout: Duration): ReaderTTaskRedisConnection[Result[Seq[String]]] =
    send(BRPopRequest(UUID.randomUUID(), keys, timeout)).flatMap {
      case BRPopSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case BRPopSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case BRPopFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  /*
   * BRPOPLPUSH
   * LINDEX
   * LINSERT
   * LLEN
   */
  override def lpop(key: String): ReaderTTaskRedisConnection[Result[Option[String]]] =
    send(LPopRequest(UUID.randomUUID(), key)).flatMap {
      case LPopSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case LPopSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case LPopFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def lpush[A: Show](key: String, value: A): ReaderTTaskRedisConnection[Result[Long]] =
    lpush(key, NonEmptyList.of(value))

  override def lpush[A: Show](key: String, values: NonEmptyList[A]): ReaderTTaskRedisConnection[Result[Long]] =
    send(LPushRequest(UUID.randomUUID(), key, values)).flatMap {
      case LPushSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case LPushSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case LPushFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }
  /*
 * LPUSHX
 * LRANGE
 * LREM
 * LSET
 * LTRIM
 * RPOP
 * RPOPLPUSH
 * RPUSH
 * RPUSHX
 */
}
