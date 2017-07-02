package com.github.j5ik2o.reactive.redis

import java.text.ParseException
import java.util.UUID

import com.github.j5ik2o.reactive.redis
import com.github.j5ik2o.reactive.redis.CommandResponseParser._

import scala.util.parsing.input.Reader

object StringsOperations {

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
    override val message: String                        = s"APPEND $key $value"
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
    override val message: String                        = s"BITCOUNT $key" + startAndEnd.fold("")(e => " " + e.start + " " + e.end)
  }

  sealed trait BitCountResponse extends Response

  case class BitCountSuspended(id: UUID, requestId: UUID) extends BitCountResponse

  case class BitCountSucceeded(id: UUID, requestId: UUID, value: Int) extends BitCountResponse

  case class BitCountFailed(id: UUID, requestId: UUID, ex: Exception) extends BitCountResponse

  // --- BITFIELD
  object BitField extends SimpleResponseFactory {

    sealed trait BitType {
      def asString: String
    }
    case class SingedBitType(bit: Int) extends BitType {
      override def asString: String = s"i$bit"
    }
    case class UnsignedBitType(bit: Int) extends BitType {
      override def asString: String = s"u$bit"
    }
    sealed trait SubOption {
      def asString: String
    }
    case class Get(bitType: BitType, offset: Int) extends SubOption {
      override def asString: String = s"${bitType.asString} $offset"
    }
    case class Set(bitType: BitType, offset: Int, value: String) extends SubOption {
      override def asString: String = s"${bitType.asString} $offset $value"
    }

    override def createResponseFromReader(requestId: UUID, message: Reader[Char]): (Response, Reader[Char]) =
      parseResponse(message) match {
        case (NumberExpr(n), next) =>
          (BitFieldSucceeded(UUID.randomUUID(), requestId, n), next)
        case (SimpleExpr("QUEUED"), next) =>
          (BitFieldSuspended(UUID.randomUUID(), requestId), next)
        case (ErrorExpr(msg), next) =>
          (BitFieldFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
        case (expr, o) =>
          logger.error("bitfield request = {}", expr)
          throw new ParseException("bitfield request", o.offset)
      }

    override protected val responseParser: redis.StringOperations.BitField.Parser[Expr] =
      numberWithCrLfOrErrorWithCrLf
  }

  case class BitField(id: UUID, key: String, option: BitField.SubOption) extends SimpleRequest {
    override val responseFactory: SimpleResponseFactory = BitField
    override val message: String = {
      s"$key ${option.asString}"
    }
  }

  sealed trait BitFieldResponse extends Response

  case class BitFieldSuspended(id: UUID, requestId: UUID) extends BitFieldResponse

  case class BitFieldSucceeded(id: UUID, requestId: UUID, value: Int) extends BitFieldResponse

  case class BitFieldFailed(id: UUID, requestId: UUID, ex: Exception) extends BitFieldResponse
  // --- BITOP

  // --- BITPOS

  // --- DECR
  object DecrRequest extends SimpleResponseFactory {
    override def createResponseFromReader(requestId: UUID, message: Reader[Char]): (Response, Reader[Char]) =
      parseResponse(message) match {
        case (NumberExpr(n), next) =>
          (DecrSucceeded(UUID.randomUUID(), requestId, n), next)
        case (SimpleExpr("QUEUED"), next) =>
          (DecrSuspended(UUID.randomUUID(), requestId), next)
        case (ErrorExpr(msg), next) =>
          (DecrFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
        case (expr, o) =>
          logger.error("decr request = {}", expr)
          throw new ParseException("decr request", o.offset)
      }

    override protected val responseParser: DecrRequest.Parser[Expr] = numberWithCrLfOrErrorWithCrLf
  }

  case class DecrRequest(id: UUID, key: String) extends SimpleRequest {
    override val responseFactory: SimpleResponseFactory = DecrRequest
    override val message: String                        = s"DECR $key"
  }

  sealed trait DecrResponse extends Response

  case class DecrSuspended(id: UUID, requestId: UUID) extends DecrResponse

  case class DecrSucceeded(id: UUID, requestId: UUID, value: Int) extends DecrResponse

  case class DecrFailed(id: UUID, requestId: UUID, ex: Exception) extends DecrResponse

  object DecrByRequest extends SimpleResponseFactory {
    override def createResponseFromReader(requestId: UUID, message: Reader[Char]): (Response, Reader[Char]) =
      parseResponse(message) match {
        case (NumberExpr(n), next) =>
          (DecrBySucceeded(UUID.randomUUID(), requestId, n), next)
        case (SimpleExpr("QUEUED"), next) =>
          (DecrBySuspended(UUID.randomUUID(), requestId), next)
        case (ErrorExpr(msg), next) =>
          (DecrByFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
        case (expr, o) =>
          logger.error("decrby request = {}", expr)
          throw new ParseException("decrby request", o.offset)
      }

    override protected val responseParser: DecrByRequest.Parser[Expr] =
      numberWithCrLfOrErrorWithCrLf
  }

  case class DecrByRequest(id: UUID, key: String, value: Int) extends SimpleRequest {
    override val responseFactory: SimpleResponseFactory = DecrByRequest
    override val message: String                        = s"DECRBY $key $value"
  }

  sealed trait DecrByResponse extends Response

  case class DecrBySuspended(id: UUID, requestId: UUID) extends DecrByResponse

  case class DecrBySucceeded(id: UUID, requestId: UUID, value: Int) extends DecrByResponse

  case class DecrByFailed(id: UUID, requestId: UUID, ex: Exception) extends DecrByResponse

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
    override val message: String                        = s"SET $key $value"
    override val responseFactory: SimpleResponseFactory = SetRequest
  }

  sealed trait SetResponse extends Response

  case class SetSuspended(id: UUID, requestId: UUID) extends SetResponse

  case class SetSucceeded(id: UUID, requestId: UUID) extends SetResponse

  case class SetFailed(id: UUID, requestId: UUID, ex: Exception) extends SetResponse

  // --- GET

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
    override val message: String                        = s"GET $key"
    override val responseFactory: SimpleResponseFactory = GetRequest
  }

  sealed trait GetResponse extends Response

  case class GetSuspended(id: UUID, requestId: UUID) extends GetResponse

  case class GetSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends GetResponse

  case class GetFailed(id: UUID, requestId: UUID, ex: Exception) extends GetResponse

  // --- GETSET

  object GetSetResponse extends SimpleResponseFactory {

    override def createResponseFromReader(requestId: UUID, message: Reader[Char]): (GetSetResponse, Reader[Char]) =
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
    override val message: String                        = s"GETSET $key $value"
    override val responseFactory: SimpleResponseFactory = GetSetResponse
  }

  sealed trait GetSetResponse extends Response

  case class GetSetSuspended(id: UUID, requestId: UUID) extends GetSetResponse

  case class GetSetSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends GetSetResponse

  case class GetSetFailed(id: UUID, requestId: UUID, ex: Exception) extends GetSetResponse

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
    override val message: String                        = s"INCR $key"
  }

  sealed trait IncrResponse extends Response

  case class IncrSuspended(id: UUID, requestId: UUID)             extends IncrResponse
  case class IncrSucceeded(id: UUID, requestId: UUID, value: Int) extends IncrResponse
  case class IncrFailed(id: UUID, requestId: UUID, ex: Exception) extends IncrResponse

  // --- INCRBY

  object IncrByRequest extends SimpleResponseFactory {
    override def createResponseFromReader(requestId: UUID, message: Reader[Char]): (Response, Reader[Char]) =
      parseResponse(message) match {
        case (NumberExpr(n), next) =>
          (IncrBySucceeded(UUID.randomUUID(), requestId, n), next)
        case (SimpleExpr("QUEUED"), next) =>
          (IncrBySuspended(UUID.randomUUID(), requestId), next)
        case (ErrorExpr(msg), next) =>
          (IncrByFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
        case (expr, o) =>
          logger.error("incrby request = {}", expr)
          throw new ParseException("incrby request", o.offset)
      }

    override protected val responseParser: Parser[Expr] = numberWithCrLfOrErrorWithCrLf
  }

  case class IncrByRequest(id: UUID, key: String, value: Int) extends SimpleRequest {
    override val responseFactory: SimpleResponseFactory = IncrByRequest
    override val message: String                        = s"INCRBY $key $value"
  }

  sealed trait IncrByResponse extends Response

  case class IncrBySuspended(id: UUID, requestId: UUID)             extends IncrByResponse
  case class IncrBySucceeded(id: UUID, requestId: UUID, value: Int) extends IncrByResponse
  case class IncrByFailed(id: UUID, requestId: UUID, ex: Exception) extends IncrByResponse

}
