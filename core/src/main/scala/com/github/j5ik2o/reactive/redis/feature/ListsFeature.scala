package com.github.j5ik2o.reactive.redis.feature

import java.util.UUID

import cats.Show
import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis._
import com.github.j5ik2o.reactive.redis.command.lists._

import scala.concurrent.duration.Duration

trait ListsFeature { this: RedisClient =>

  def blpop(keys: NonEmptyList[String], timeout: Duration): ReaderTTaskRedisConnection[Result[Seq[String]]] =
    send(BLPopRequest(UUID.randomUUID(), keys, timeout)).flatMap {
      case BLPopSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case BLPopSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case BLPopFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def brpop(keys: NonEmptyList[String], timeout: Duration): ReaderTTaskRedisConnection[Result[Seq[String]]] =
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
  def lpop(key: String): ReaderTTaskRedisConnection[Result[Option[String]]] =
    send(LPopRequest(UUID.randomUUID(), key)).flatMap {
      case LPopSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case LPopSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case LPopFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def lpush[A: Show](key: String, value: A): ReaderTTaskRedisConnection[Result[Int]] =
    lpush(key, NonEmptyList.of(value))

  def lpush[A: Show](key: String, values: NonEmptyList[A]): ReaderTTaskRedisConnection[Result[Int]] =
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
