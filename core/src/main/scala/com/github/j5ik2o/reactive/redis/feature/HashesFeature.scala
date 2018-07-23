package com.github.j5ik2o.reactive.redis.feature

import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis._
import com.github.j5ik2o.reactive.redis.command.hashes._

trait HashesFeature {
  this: RedisClient =>

  def hdel(key: String, field: String): ReaderTTaskRedisConnection[Result[Int]] =
    hdel(key, NonEmptyList.of(field))

  def hdel(key: String, fields: NonEmptyList[String]): ReaderTTaskRedisConnection[Result[Int]] =
    send(HDelRequest(UUID.randomUUID(), key, fields)).flatMap {
      case HDelSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case HDelSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case HDelFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def hexists(key: String, field: String): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(HExistsRequest(UUID.randomUUID(), key, field)).flatMap {
      case HExistsSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case HExistsSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case HExistsFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def hget(key: String, field: String): ReaderTTaskRedisConnection[Result[Option[String]]] =
    send(HGetRequest(UUID.randomUUID(), key, field)).flatMap {
      case HGetSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case HGetSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case HGetFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }
  /* HGETALL
   * HINCRBY
   * HINCRBYFLOAT
   * HKEYS
   * HLEN
   * HMGET
   * HMSET
   * HSCAN
   */
  def hset(key: String, field: String, value: String): ReaderTTaskRedisConnection[Result[Boolean]] =
    send(HSetRequest(UUID.randomUUID(), key, field, value)).flatMap {
      case HSetSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case HSetSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case HSetFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }
  /*
 * HSETNX
 * HSTRLEN
 * HVALS
 */
}
