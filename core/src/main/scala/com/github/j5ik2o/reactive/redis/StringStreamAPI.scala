package com.github.j5ik2o.reactive.redis

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.github.j5ik2o.reactive.redis.StringClient.Protocol.String.SetRequest
import com.github.j5ik2o.reactive.redis.connection.ConnectionStreamAPI
import com.github.j5ik2o.reactive.redis.keys.KeysStreamAPI
import com.github.j5ik2o.reactive.redis.server.ServerStreamAPI

import scala.concurrent.{ ExecutionContext, Future }

trait StringStreamAPI extends BaseStreamAPI {

  import ResponseRegexs._

  def set(key: String, value: String): Source[SetRequest, NotUsed] =
    Source.single(SetRequest(key, value))

//  def set(key: String, value: String)(implicit mat: Materializer, ec: ExecutionContext): Future[Unit] = {
//    setSource(key, value).log("request").via(toByteStringFlow).via(connection).runWith(sink).map{ v =>
//      v.head match {
//        case simpleStringRegex(_) =>
//          ()
//        case _ =>
//          throw RedisIOException(Some(v.head))
//      }
//    }
//  }

  def getSource(key: String): Source[String, NotUsed] =
    Source.single(s"GET $key")

  def get(key: String)(implicit mat: Materializer, ec: ExecutionContext): Future[Option[String]] = {
    getSource(key).log("request").via(toByteStringFlow).via(connection).runWith(sink).map{ v =>
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
  }

  private def getSetSource(key: String, value: String): Source[String, NotUsed] =
    Source.single(s"GETSET $key $value")

  def getSet(key: String, value: String)(implicit mat: Materializer, ec: ExecutionContext): Future[String] = {
    getSetSource(key, value).log("request").via(toByteStringFlow).via(connection).runWith(sink).map{ v =>
      v.head match {
        case dollorRegex(s) =>
          v(1)
        case _ =>
          throw RedisIOException(Some(v.head))
      }
    }
  }

}
