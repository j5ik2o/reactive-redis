package com.github.j5ik2o.reactive.redis.feature

import java.util.UUID

import cats.Show
import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis._
import com.github.j5ik2o.reactive.redis.command.lists._

import scala.concurrent.duration.Duration

trait ListsAPI[M[_]] {
  def blPop(timeout: Duration, key: String, keys: String*): M[Result[Seq[String]]]

  def blPop(timeout: Duration, keys: NonEmptyList[String]): M[Result[Seq[String]]]

  def brPop(timeout: Duration, key: String, keys: String*): M[Result[Seq[String]]]

  def brPop(timeout: Duration, keys: NonEmptyList[String]): M[Result[Seq[String]]]

  def brPopLPush(source: String, destination: String, timeout: Duration): M[Result[String]]

  def lPop(key: String): M[Result[Option[String]]]

  def lPush[A: Show](key: String, value: A, values: A*): M[Result[Long]]

  def lPush[A: Show](key: String, values: NonEmptyList[A]): M[Result[Long]]

  def lRange(key: String, start: Long, stop: Long): M[Result[Seq[String]]]

  def rPush[A: Show](key: String, value: A, values: A*): M[Result[Long]]

  def rPush[A: Show](key: String, values: NonEmptyList[A]): M[Result[Long]]
}

trait ListsFeature extends ListsAPI[ReaderTTaskRedisConnection] {
  this: RedisClient =>

  override def blPop(timeout: Duration, key: String, keys: String*): ReaderTTaskRedisConnection[Result[Seq[String]]] =
    blPop(timeout, NonEmptyList.of[String](key, keys: _*))

  override def blPop(timeout: Duration, keys: NonEmptyList[String]): ReaderTTaskRedisConnection[Result[Seq[String]]] =
    send(BLPopRequest(UUID.randomUUID(), keys, timeout)).flatMap {
      case BLPopSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case BLPopSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case BLPopFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def brPop(timeout: Duration, key: String, keys: String*): ReaderTTaskRedisConnection[Result[Seq[String]]] =
    brPop(timeout, NonEmptyList.of[String](key, keys: _*))

  override def brPop(timeout: Duration, keys: NonEmptyList[String]): ReaderTTaskRedisConnection[Result[Seq[String]]] =
    send(BRPopRequest(UUID.randomUUID(), keys, timeout)).flatMap {
      case BRPopSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case BRPopSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case BRPopFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def brPopLPush(source: String,
                          destination: String,
                          timeout: Duration): ReaderTTaskRedisConnection[Result[String]] =
    send(BRPopLPushRequest(UUID.randomUUID(), source, destination, timeout)).flatMap {
      case BRPopLPushSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case BRPopLPushSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case BRPopLPushFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  /*
   * LINDEX
   * LINSERT
   */

  def lLen(key: String): ReaderTTaskRedisConnection[Result[Long]] =
    send(LLenRequest(UUID.randomUUID(), key)).flatMap {
      case LLenSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case LLenSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case LLenFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def lPop(key: String): ReaderTTaskRedisConnection[Result[Option[String]]] =
    send(LPopRequest(UUID.randomUUID(), key)).flatMap {
      case LPopSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case LPopSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case LPopFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def lPush[A: Show](key: String, value: A, values: A*): ReaderTTaskRedisConnection[Result[Long]] =
    lPush(key, NonEmptyList.of(value, values: _*))

  override def lPush[A: Show](key: String, values: NonEmptyList[A]): ReaderTTaskRedisConnection[Result[Long]] =
    send(LPushRequest(UUID.randomUUID(), key, values)).flatMap {
      case LPushSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case LPushSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case LPushFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  /**
    * LPUSHX
    */
  override def lRange(key: String, start: Long, stop: Long): ReaderTTaskRedisConnection[Result[Seq[String]]] =
    send(LRangeRequest(UUID.randomUUID(), key, start, stop)).flatMap {
      case LRangeSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case LRangeSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case LRangeFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  /*
   * LREM
   * LSET
   * LTRIM
   * RPOP
   * RPOPLPUSH
   */
  override def rPush[A: Show](key: String, value: A, values: A*): ReaderTTaskRedisConnection[Result[Long]] =
    rPush(key, NonEmptyList.of(value, values: _*))

  override def rPush[A: Show](key: String, values: NonEmptyList[A]): ReaderTTaskRedisConnection[Result[Long]] =
    send(RPushRequest(UUID.randomUUID(), key, values)).flatMap {
      case RPushSuspend(_, _)          => ReaderTTask.pure(Suspended)
      case RPushSucceeded(_, _, value) => ReaderTTask.pure(Provided(value))
      case RPushFailed(_, _, ex)       => ReaderTTask.raiseError(ex)
    }

  /*
 * RPUSHX
 */
}
