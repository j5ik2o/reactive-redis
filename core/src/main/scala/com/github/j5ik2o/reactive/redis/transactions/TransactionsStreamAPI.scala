package com.github.j5ik2o.reactive.redis.transactions

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.{ BaseStreamAPI, RedisIOException }

import scala.concurrent.{ ExecutionContext, Future }

trait TransactionsStreamAPI extends BaseStreamAPI {
  import com.github.j5ik2o.reactive.redis.ResponseRegexs._
  // --- DISCARD
  private val discardSource: Source[String, NotUsed] = Source.single("DISCARD")

  def discard(implicit mat: Materializer, ec: ExecutionContext): Future[Unit] = {
    discardSource.log("request").via(toByteStringFlow).via(connection).runWith(sink).map { v =>
      v.head match {
        case simpleStringRegex(s) =>
          ()
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case m =>
          throw parseException(Some(m))
      }
    }
  }

  // --- EXEC
  private val execSource: Source[String, NotUsed] = Source.single("EXEC")

  def exec(implicit mat: Materializer, ec: ExecutionContext): Future[Seq[String]] = {
    execSource.log("request").via(toByteStringFlow).via(connection).runWith(sink).map { v =>
      v.head match {
        case dollorRegex(d) =>
          v.tail.filterNot {
            case dollorRegex(_) => true
            case _ => false
          }
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case m =>
          throw parseException(Some(m))
      }
    }
  }

  // --- MULTI
  private def multiSource: Source[String, NotUsed] = {
    val t = Source.single("MULTI")
    t
  }

  def multi(commands: Source[String, NotUsed])(implicit mat: Materializer, ec: ExecutionContext): Future[Unit] = {
    multiSource.concat(commands).concat(execSource).log("request")
      .fold(ByteString.empty) { (acc, in) =>
        acc ++ ByteString(in.concat("\r\n"))
      }.via(connection).runWith(sink).map { v =>
      v.head match {
        case simpleStringRegex(s) =>
          ()
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case m =>
          throw parseException(Some(m))
      }
    }

    //      .flatMapConcat(e => e.foldLeft(ByteString.empty)((acc, in) => acc :+ ByteString(in))).via(connection).runWith(sink).map { v =>
    //      v.head match {
    //        case stringRegex(s) =>
    //          ()
    //        case errorRegex(msg) =>
    //          throw RedisIOException(Some(msg))
    //        case m =>
    //          throw parseException(Some(m))
    //      }
    //    }
  }

  // --- UNWATCH
  private val unwatchSource: Source[String, NotUsed] = Source.single("UNWATCH")

  def unwatch(implicit mat: Materializer, ec: ExecutionContext): Future[Unit] = {
    unwatchSource.log("request").via(toByteStringFlow).via(connection).runWith(sink).map { v =>
      v.head match {
        case simpleStringRegex(s) =>
          ()
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case m =>
          throw parseException(Some(m))
      }
    }
  }

  // --- WATCH
  private val watchSource: Source[String, NotUsed] = Source.single("WATCH")

  def watch(implicit mat: Materializer, ec: ExecutionContext): Future[Unit] = {
    watchSource.log("request").via(toByteStringFlow).via(connection).runWith(sink).map { v =>
      v.head match {
        case simpleStringRegex(s) =>
          ()
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case m =>
          throw parseException(Some(m))
      }
    }
  }
}
