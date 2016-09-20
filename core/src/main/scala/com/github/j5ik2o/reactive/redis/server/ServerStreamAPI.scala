package com.github.j5ik2o.reactive.redis.server

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.github.j5ik2o.reactive.redis.BaseStreamAPI
import com.github.j5ik2o.reactive.redis.server.ServerProtocol._

trait ServerStreamAPI extends BaseStreamAPI {
  // --- BGREWRITEAOF
  // --- BGSAVE
  val bgSaveRequest = Source.single(BgSaveRequest)

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
  val dbSizeRequest: Source[DBSizeRequest.type, NotUsed] = Source.single(DBSizeRequest)

  // --- DEBUG OBJECT
  // --- DEBUG SEGFAULT

  // --- FLUSHALL
  val flushAllRequest: Source[FlushAllRequest.type, NotUsed] = Source.single(FlushAllRequest)

  // --- FLUSHDB
  val flushDB: Source[FlushDBRequest.type, NotUsed] = Source.single(FlushDBRequest)

  // --- INFO
  val infoRequest: Source[InfoRequest.type, NotUsed] = Source.single(InfoRequest)

  // --- LASTSAVE

  // --- MONITOR

  // --- ROLE

  // --- SAVE

  // --- SHUTDOWN
  def shutdown(save: Boolean = true): Source[ShutdownRequest, NotUsed] = Source.single(ShutdownRequest(save))

  // --- SLAVEOF

  // --- SLOWLOG

  // --- SYNC

  // --- TIME
  val time: Source[TimeRequest.type, NotUsed] = Source.single(TimeRequest)


}
