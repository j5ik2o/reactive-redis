package com.github.j5ik2o.reactive.redis

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString

import scala.concurrent.{ ExecutionContext, Future }

trait StringStreamApi extends CommonStreamApi {

  private def setSource(key: String, value: String): Source[ByteString, NotUsed] =
    Source.single(ByteString(s"SET $key $value\r\n"))

  def set(key: String, value: String)(implicit mat: Materializer, ec: ExecutionContext): Future[Unit] =
    connection.runWith(setSource(key, value), sink)._2.map { v =>
      v.head match {
        case stringRegex(_) =>
          ()
        case _ =>
          throw RedisIOException(Some(v.head))
      }
    }

  private def getSource(key: String): Source[ByteString, NotUsed] =
    Source.single(ByteString(s"GET $key\r\n"))

  def get(key: String)(implicit mat: Materializer, ec: ExecutionContext): Future[Option[String]] =
    connection.runWith(getSource(key), sink)._2.map { v =>
      v.head match {
        case dollorRegex(n) =>
          if (n.toInt == -1)
            None
          else
            Some(v(1))
        case _ =>
          throw RedisIOException(Some(v.head))
      }
    }

  private def getSetSource(key: String, value: String): Source[ByteString, NotUsed] =
    Source.single(ByteString(s"GETSET $key $value\r\n"))

  def getSet(key: String, value: String)(implicit mat: Materializer, ec: ExecutionContext): Future[String] =
    connection.runWith(getSetSource(key, value), sink)._2.map { v =>
      v.head match {
        case dollorRegex(s) =>
          v(1)
        case _ =>
          throw RedisIOException(Some(v.head))
      }
    }
}
