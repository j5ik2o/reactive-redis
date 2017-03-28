package com.github.j5ik2o.reactive.redis

import java.text.ParseException
import java.util.UUID

import com.github.j5ik2o.reactive.redis.CommandResponseParser._
import com.github.j5ik2o.reactive.redis.StringOperations.IncrRequest

import scala.util.parsing.input.Reader

object StringOperations {

  import Options._

  // --- APPEND

  object AppendRequest extends SimpleResponseFactory {
    override def createResponseFromReader(requestId: UUID, message: Reader[Char]): (Response, Reader[Char]) =
      parseResponse(message) match {
        case (NumberExpr(n), next) =>
          (AppendSucceeded(UUID.randomUUID(), requestId, n), next)
        case (SimpleExpr("QUEUED"), next) =>
          (AppendSuspended(UUID.randomUUID(), requestId), next)
        case (ErrorExpr(msg), next) =>
          (AppendFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
        case (expr, o) =>
          logger.error("append request = {}", expr)
          throw new ParseException("append request", o.offset)
      }

    override protected val responseParser: Parser[Expr] = numberWithCrLfOrErrorWithCrLf
  }

  case class AppendRequest(id: UUID, key: String, value: String) extends SimpleRequest {
    override val responseFactory: SimpleResponseFactory = AppendRequest
    override val message: String = s"APPEND $key $value"
  }

  sealed trait AppendResponse extends Response

  case class AppendSuspended(id: UUID, requestId: UUID) extends AppendResponse

  case class AppendSucceeded(id: UUID, requestId: UUID, value: Int) extends AppendResponse

  case class AppendFailed(id: UUID, requestId: UUID, ex: Exception) extends AppendResponse

  // --- BITCOUNT

  object BitCountRequest extends SimpleResponseFactory {
    override def createResponseFromReader(requestId: UUID, message: Reader[Char]): (Response, Reader[Char]) =
      parseResponse(message) match {
        case (NumberExpr(n), next) =>
          (BitCountSucceeded(UUID.randomUUID(), requestId, n), next)
        case (SimpleExpr("QUEUED"), next) =>
          (BitCountSuspended(UUID.randomUUID(), requestId), next)
        case (ErrorExpr(msg), next) =>
          (BitCountFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
        case (expr, o) =>
          logger.error("bitcount request = {}", expr)
          throw new ParseException("bitcount request", o.offset)
      }

    override protected val responseParser: Parser[Expr] = numberWithCrLfOrErrorWithCrLf
  }

  case class BitCountRequest(id: UUID, key: String, startAndEnd: Option[StartAndEnd] = None) extends SimpleRequest {
    override val responseFactory: SimpleResponseFactory = BitCountRequest
    override val message: String = s"BITCOUNT $key" + startAndEnd.fold("")(e => " " + e.start + " " + e.end)
  }

  sealed trait BitCountResponse extends Response

  case class BitCountSuspended(id: UUID, requestId: UUID) extends BitCountResponse

  case class BitCountSucceeded(id: UUID, requestId: UUID, value: Int) extends BitCountResponse

  case class BitCountFailed(id: UUID, requestId: UUID, ex: Exception) extends BitCountResponse

  // --- SET

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

  // ---

  object GetSetSimpleResponse extends SimpleResponseFactory {

    override def createResponseFromReader(requestId: UUID, message: Reader[Char]): (GetSetSimpleResponse, Reader[Char]) =
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
    override val responseFactory: SimpleResponseFactory = GetSetSimpleResponse
  }

  sealed trait GetSetSimpleResponse extends Response

  case class GetSetSuspended(id: UUID, requestId: UUID) extends GetSetSimpleResponse

  case class GetSetSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends GetSetSimpleResponse

  case class GetSetFailed(id: UUID, requestId: UUID, ex: Exception) extends GetSetSimpleResponse

  // --- INCR

  object IncrRequest extends SimpleResponseFactory {
    override def createResponseFromReader(requestId: UUID, message: Reader[Char]): (Response, Reader[Char]) =
      parseResponse(message) match {
        case (NumberExpr(n), next) =>
          (IncrSucceeded(UUID.randomUUID(), requestId, n), next)
        case (SimpleExpr("QUEUED"), next) =>
          (IncrSuspended(UUID.randomUUID(), requestId), next)
        case (ErrorExpr(msg), next) =>
          (IncrFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
        case (expr, o) =>
          logger.error("incr request = {}", expr)
          throw new ParseException("incr request", o.offset)
      }

    override protected val responseParser: Parser[Expr] = numberWithCrLfOrErrorWithCrLf
  }

  case class IncrRequest(id: UUID, key: String) extends SimpleRequest {
    override val responseFactory: SimpleResponseFactory = IncrRequest
    override val message: String = s"INCR $key"
  }

  sealed trait IncrResponse extends Response

  case class IncrSuspended(id: UUID, requestId: UUID) extends IncrResponse
  case class IncrSucceeded(id: UUID, requestId: UUID, value: Int) extends IncrResponse
  case class IncrFailed(id: UUID, requestId: UUID, ex: Exception) extends IncrResponse

}
