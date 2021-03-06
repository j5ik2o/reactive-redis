package com.github.j5ik2o.reactive.redis.feature

import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis._
import com.github.j5ik2o.reactive.redis.command.hashes._

trait HashesAPI[M[_]] {
  def hdel(key: String, field: String): M[Result[Long]]
  def hdel(key: String, fields: NonEmptyList[String]): M[Result[Long]]
  def hexists(key: String, field: String): M[Result[Boolean]]
  def hget(key: String, field: String): M[Result[Option[String]]]
  def hgetAll(key: String): M[Result[Seq[String]]]
  def hset(key: String, field: String, value: String): M[Result[Boolean]]
  def hsetNx(key: String, field: String, value: String): M[Result[Boolean]]
}

trait HashesFeature extends HashesAPI[ReaderTTaskRedisConnection] {
  this: RedisClient =>

  override def hdel(key: String, field: String): ReaderTTaskRedisConnection[Result[Long]] =
    hdel(key, NonEmptyList.of(field))

  override def hdel(key: String, fields: NonEmptyList[String]): ReaderTTaskRedisConnection[Result[Long]] =
    send(HDelRequest(UUID.randomUUID(), key, fields)).flatMap {
      case HDelSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case HDelSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case HDelFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def hexists(key: String, field: String): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(HExistsRequest(UUID.randomUUID(), key, field)).flatMap {
      case HExistsSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case HExistsSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case HExistsFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def hget(key: String, field: String): ReaderTTaskRedisConnection[Result[Option[String]]] =
    send(HGetRequest(UUID.randomUUID(), key, field)).flatMap {
      case HGetSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case HGetSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case HGetFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def hgetAll(key: String): ReaderTTaskRedisConnection[Result[Seq[String]]] =
    send(HGetAllRequest(UUID.randomUUID(), key)).flatMap {
      case HGetAllSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case HGetAllSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case HGetAllFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }
  /*
   * HINCRBY
   * HINCRBYFLOAT
   * HKEYS
   * HLEN
   * HMGET
   * HMSET
   * HSCAN
   */
  override def hset(key: String, field: String, value: String): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(HSetRequest(UUID.randomUUID(), key, field, value)).flatMap {
      case HSetSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case HSetSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case HSetFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def hsetNx(key: String, field: String, value: String): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(HSetNxRequest(UUID.randomUUID(), key, field, value)).flatMap {
      case HSetNxSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case HSetNxSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case HSetNxFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  /*
 * HSTRLEN
 * HVALS
 */
}
