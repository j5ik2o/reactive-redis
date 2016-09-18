package com.github.j5ik2o.reactive.redis.server

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.github.j5ik2o.reactive.redis.{ BaseStreamAPI, RedisIOException }

import scala.concurrent.{ ExecutionContext, Future }

trait ServerStreamAPI extends BaseStreamAPI {
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
  private val dbSizeSource: Source[String, NotUsed] = Source.single("DBSIZE")

  def dbSize(implicit mat: Materializer, ec: ExecutionContext): Future[Int] = {
    dbSizeSource.log("request").via(toByteString).via(connection).runWith(sink).map { v =>
      v.head match {
        case digitsRegex(d) =>
          d.toInt
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }
  }

  // --- DEBUG OBJECT
  // --- DEBUG SEGFAULT

  // --- FLUSHALL
  private val flushAllSource: Source[String, NotUsed] = Source.single("FLUSHALL")

  def flushAll(implicit mat: Materializer, ec: ExecutionContext): Future[Unit] = {
    flushAllSource.log("request").via(toByteString).via(connection).runWith(sink).map { v =>
      v.head match {
        case stringRegex(s) =>
          require(s == "OK")
          ()
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }
  }

  // --- FLUSHDB
  private val flushDBSource: Source[String, NotUsed] = Source.single("FLUSHDB")

  def flushDB(implicit mat: Materializer, ec: ExecutionContext): Future[Unit] = {
    flushDBSource.log("request").via(toByteString).via(connection).runWith(sink).map { v =>
      v.head match {
        case stringRegex(s) =>
          require(s == "OK")
          ()
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }
  }

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
