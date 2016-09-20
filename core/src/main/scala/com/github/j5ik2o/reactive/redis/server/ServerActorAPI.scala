package com.github.j5ik2o.reactive.redis.server

import akka.actor.Actor
import com.github.j5ik2o.reactive.redis.BaseActorAPI
import akka.pattern.{ ask, pipe }

trait ServerActorAPI extends BaseActorAPI with ServerCommandRequests {
  this: Actor =>

  import ServerProtocol._

  import context.dispatcher

  val handleServer: Receive = {
    // --- BGREWRITEAOF
    // --- BGSAVE
    // --- CLIENT GETNAME
    // --- CLIENT KILL
    // --- CLIENT LIST
    // --- CLIENT PAUSE
    // --- CLIENT REPLY
    // --- CLIENT SETNAME
    // --- COMMAND
    // --- COMMAND COUNT
    // --- COMMAND GETKEYS
    // --- COMMAND INFO
    // --- CONFIG GET
    // --- CONFIG RESETSTAT
    // --- CONFIG REWRITE
    // --- CONFIG SET

    // --- DBSIZE
    case DBSizeRequest   =>
    //run(dbSize).pipeTo(sender())

    // --- DEBUG OBJECT
    // --- DEBUG SEGFAULT

    // --- FLUSHALL
    case FlushAllRequest =>
    //run(flushAll).pipeTo(sender())

    // --- FLUSHDB
    case FlushDBRequest  =>
    //run(flushDB).pipeTo(sender())

    // --- INFO
    // --- LASTSAVE
    // --- MONITOR
    // --- ROLE
    // --- SAVE
    // --- SHUTDOWN
    // --- SLAVEOF
    // --- SLOWLOG
    // --- SYNC
    // --- TIME
  }
}
