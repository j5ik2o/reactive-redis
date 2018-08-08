package com.github.j5ik2o.reactive.redis.feature

import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis._
import com.github.j5ik2o.reactive.redis.command.sets.{ SAddFailed, SAddRequest, SAddSucceeded, SAddSuspended }

trait SetsFeature { this: RedisClient =>

  def sAdd(key: String, member: String, members: String*): ReaderTTaskRedisConnection[Result[Long]] =
    sAdd(key, NonEmptyList.of(member, members: _*))

  def sAdd(key: String, members: NonEmptyList[String]): ReaderTTaskRedisConnection[Result[Long]] =
    send(SAddRequest(UUID.randomUUID(), key, members)).flatMap {
      case SAddSuspended(_, _)        => ReaderTTask.pure(Suspended)
      case SAddSucceeded(_, _, value) => ReaderTTask.pure(Provided(value))
      case SAddFailed(_, _, ex)       => ReaderTTask.raiseError(ex)
    }

  /*
 * SCARD
 * SDIFF
 * SDIFFSTORE
 * SINTER
 * SINTERSTORE
 * SISMEMBER
 * SMEMBERS
 * SMOVE
 * SPOP
 * SRANDMEMBER
 * SREM
 * SSCAN
 * SUNION
 * SUNIONSTORE
 */
}
