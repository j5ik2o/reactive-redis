package com.github.j5ik2o.reactive.redis.feature

import java.util.UUID

import com.github.j5ik2o.reactive.redis._
import com.github.j5ik2o.reactive.redis.command.hashes._

trait HashesFeature {
  this: RedisClient =>

  /**
    * HDEL
    * HEXISTS
    */
  def hget(key: String, field: String): ReaderTTaskRedisConnection[Result[String]] =
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
