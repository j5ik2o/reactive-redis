package com.github.j5ik2o.reactive.redis.server

object ServerProtocol {
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
  case object DBSizeRequest

  case class DBSizeSucceeded(value: Int)

  case class DBSizeFailure(ex: Exception)

  // --- DEBUG OBJECT
  // --- DEBUG SEGFAULT

  // --- FLUSHALL
  case object FlushAllRequest

  case object FlushAllSucceeded

  case class FlushAllFailure(ex: Exception)

  // --- FLUSHDB
  case object FlushDBRequest

  case object FlushDBSucceeded

  case class FlushDBFailure(ex: Exception)


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
