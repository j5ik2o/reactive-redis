package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.text.ParseException
import java.util.UUID

import akka.actor.Props
import akka.io.Inet.SocketOption
import com.github.j5ik2o.reactive.redis.CommandResponseParser.{ ErrorExpr, Expr, SimpleExpr, StringOptExpr }

import scala.collection.immutable
import scala.concurrent.duration.Duration
import scala.util.parsing.input.Reader

object StringOperations {

  object SetRequest extends SimpleResponseFactory {
    override def createResponseFromReader(requestId: UUID, message: Reader[Char]): (Response, Reader[Char]) =
      parseResponse(message) match {
        case (SimpleExpr("OK"), next) =>
          (SetSucceeded(UUID.randomUUID(), requestId), next)
        case (SimpleExpr("QUEUED"), next) =>
          (SetSuspended(UUID.randomUUID(), requestId), next)
        case (ErrorExpr(msg), next) =>
          (SetFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
        case (expr, o) =>
          logger.error("set request = {}", expr)
          throw new ParseException("set request", o.offset)
      }

    override protected val responseParser: Parser[Expr] = simpleWithCrLfOrErrorWithCrLf
  }

  case class SetRequest(id: UUID, key: String, value: String) extends SimpleRequest {
    override val message: String = s"SET $key $value"
    override val responseFactory: SimpleResponseFactory = SetRequest
  }

  sealed trait SetResponse extends Response

  case class SetSuspended(id: UUID, requestId: UUID) extends SetResponse

  case class SetSucceeded(id: UUID, requestId: UUID) extends SetResponse

  case class SetFailed(id: UUID, requestId: UUID, ex: Exception) extends SetResponse

  // ---

  object GetRequest extends SimpleResponseFactory {
    override def createResponseFromReader(requestId: UUID, message: Reader[Char]): (Response, Reader[Char]) = {
      parseResponse(message) match {
        case (StringOptExpr(s), next) =>
          (GetSucceeded(UUID.randomUUID(), requestId, s), next)
        case (SimpleExpr("QUEUED"), next) =>
          (GetSuspended(UUID.randomUUID(), requestId), next)
        case (ErrorExpr(msg), next) =>
          (GetFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
        case (expr, o) =>
          logger.error("get request = {}", expr)
          throw new ParseException("get request", o.offset)
      }
    }
    override protected val responseParser: Parser[Expr] =
      simpleWithCrLfOrErrorWithCrLf | bulkStringWithCrLfOrErrorWithCrLf
  }

  case class GetRequest(id: UUID, key: String) extends SimpleRequest {
    override val message: String = s"GET $key"
    override val responseFactory: SimpleResponseFactory = GetRequest
  }

  sealed trait GetResponse extends Response

  case class GetSuspended(id: UUID, requestId: UUID) extends GetResponse

  case class GetSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends GetResponse

  case class GetFailed(id: UUID, requestId: UUID, ex: Exception) extends GetResponse

  object RedisActor {

    def props(host: String, port: Int): Props =
      props(new InetSocketAddress(host, port))

    def props(
      remoteAddress:  InetSocketAddress,
      localAddress:   Option[InetSocketAddress]           = None,
      options:        immutable.Traversable[SocketOption] = Nil,
      halfClose:      Boolean                             = true,
      connectTimeout: Duration                            = Duration.Inf,
      idleTimeout:    Duration                            = Duration.Inf
    ): Props =
      Props(new RedisActor(remoteAddress, localAddress, options, halfClose, connectTimeout, idleTimeout))

  }

  // ---

  object GetSetSimpleResponse$ extends SimpleResponseFactory {

    override def createResponseFromReader(requestId: UUID, message: Reader[Char]): (GetSetSimpleResponse$, Reader[Char]) =
      parseResponse(message) match {
        case (StringOptExpr(s), next) =>
          (GetSetSucceeded(UUID.randomUUID(), requestId, s), next)
        case (SimpleExpr("QUEUED"), next) =>
          (GetSetSuspended(UUID.randomUUID(), requestId), next)
        case (ErrorExpr(msg), next) =>
          (GetSetFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
        case (expr, o) =>
          logger.error("get set request = {}", expr)
          throw new ParseException("get set request", o.offset)
      }

    override protected val responseParser: Parser[Expr] =
      simpleWithCrLfOrErrorWithCrLf | bulkStringWithCrLfOrErrorWithCrLf

  }

  case class GetSetRequest(id: UUID, key: String, value: String) extends SimpleRequest {
    override val message: String = s"GETSET $key $value"
    override val responseFactory: SimpleResponseFactory = GetSetSimpleResponse$
  }

  sealed trait GetSetSimpleResponse$ extends Response

  case class GetSetSuspended(id: UUID, requestId: UUID) extends GetSetSimpleResponse$

  case class GetSetSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends GetSetSimpleResponse$

  case class GetSetFailed(id: UUID, requestId: UUID, ex: Exception) extends GetSetSimpleResponse$

}
