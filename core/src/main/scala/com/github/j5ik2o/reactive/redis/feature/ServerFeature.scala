package com.github.j5ik2o.reactive.redis.feature

import java.util.UUID

import com.github.j5ik2o.reactive.redis._
import com.github.j5ik2o.reactive.redis.command.server.{
  FlushAllFailed,
  FlushAllRequest,
  FlushAllSucceeded,
  FlushAllSuspended
}

trait ServerFeature {
  this: RedisClient =>

  /*
   * BGREWRITEAOF
   * BGSAVE
   * CLIENT GETNAME
   * CLIENT KILL
   * CLIENT LIST
   * CLIENT PAUSE
   * CLIENT REPLY
   * CLIENT SETNAME
   * COMMAND
   * COMMAND COUNT
   * COMMAND GETKEYS
   * COMMAND INFO
   * CONFIG GET
   * CONFIG RESETSTAT
   * CONFIG REWRITE
   * CONFIG SET
   * DBSIZE
   * DEBUG OBJECT
   * DEBUG SEGFAULT
   */

  def flushAll(async: Boolean = false): ReaderTTaskRedisConnection[Unit] =
    send(FlushAllRequest(UUID.randomUUID(), async)).flatMap {
      case FlushAllSuspended(_, _)  => ReaderTTask.pure(Suspended)
      case FlushAllSucceeded(_, _)  => ReaderTTask.pure(Provided(()))
      case FlushAllFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

  /*
 * FLUSHDB
 * INFO
 * LASTSAVE
 * MEMORY DOCTOR
 * MEMORY HELP
 * MEMORY MALLOC-STATS
 * MEMORY PURGE
 * MEMORY STATS
 * MEMORY USAGE
 * MONITOR
 * ROLE
 * SAVE
 * SHUTDOWN
 * SLAVEOF
 * SLOWLOG
 * SYNC
 * TIME
 */

}
