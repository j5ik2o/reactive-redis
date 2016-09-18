package com.github.j5ik2o.reactive.redis.connection

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{ Keep, Sink, Source }
import com.github.j5ik2o.reactive.redis.{ BaseStreamAPI, RedisIOException }

import scala.concurrent.{ ExecutionContext, Future }

trait ConnectionStreamAPI extends BaseStreamAPI {

  // --- AUTH

  // --- ECHO

  // --- PING

  // --- QUIT
  private val quitSource: Source[String, NotUsed] = Source.single("QUIT")

  def quit(implicit mat: Materializer): Future[Unit] = {
    quitSource.log("request").via(toByteString).via(connection).map(_ => ()).toMat(Sink.last)(Keep.right).run()
  }

  // --- SELECT
  private def selectSource(index: Int) = Source.single(s"SELECT $index")

  def select(index: Int)(implicit mat: Materializer, ec: ExecutionContext): Future[Unit] = {
    selectSource(index).log("request").via(toByteString).via(connection).runWith(sink).map { v =>
      v.head match {
        case stringRegex(_) =>
          ()
        case errorRegex(msg) =>
          throw RedisIOException(Some(msg))
        case _ =>
          throw parseException
      }
    }
  }


}
