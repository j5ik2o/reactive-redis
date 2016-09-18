package com.github.j5ik2o.reactive.redis.server

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.github.j5ik2o.reactive.redis.{ BaseStreamAPI, RedisIOException }

import scala.concurrent.{ ExecutionContext, Future }

trait ServerStreamAPI extends BaseStreamAPI {
  // --- BGREWRITEAOF
  // --- BGSAVE
  private val bgSaveSource: Source[String, NotUsed] = Source.single("BGSAVE")

  def bgSave(implicit mat: Materializer, ec: ExecutionContext): Future[Unit] = {
    bgSaveSource.log("request").via(toByteString).via(connection).runWith(sink).map { v =>
      v.head match {
        case messageRegex(s) =>
          ()
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }
  }


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
  private val infoSource: Source[String, NotUsed] = Source.single("INFO")

  def info(implicit mat: Materializer, ec: ExecutionContext): Future[Seq[String]] = {
    infoSource.log("request").via(toByteString).via(connection).runWith(sink).map { v =>
      v.head match {
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          v
      }
    }
  }

  // --- LASTSAVE

  // --- MONITOR

  // --- ROLE

  // --- SAVE

  // --- SHUTDOWN
  private def shutdownSource(save: Option[Boolean]): Source[String, NotUsed] = Source.single(s"SHUTDOWN ${
    save.map {
      case true => "SAVE"
      case false => "NOSAVE"
    }.getOrElse("SAVE")
  }")

  def shutdown(save: Option[Boolean] = Some(true))(implicit mat: Materializer, ec: ExecutionContext): Future[Unit] = {
    shutdownSource(save).log("request").via(toByteString).via(connection).runWith(sink).map { v =>
      v.headOption match {
        case Some(errorRegex(msg)) =>
          throw RedisIOException(Some(msg))
        case _ =>
          ()
      }
    }
  }

  // --- SLAVEOF

  // --- SLOWLOG

  // --- SYNC

  // --- TIME
  private val timeSource: Source[String, NotUsed] = Source.single("TIME")

  def time(implicit mat: Materializer, ec: ExecutionContext): Future[Seq[Int]] = {
    timeSource.log("request").via(toByteString).via(connection).runWith(sink).map { v =>
      v.head match {
        case listSizeRegex(d) =>
          v.tail.filterNot {
            case dollorRegex(_) => true
            case _ => false
          }.map(_.toInt)
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }
  }

}
