package com.github.j5ik2o.reactive.redis

import java.text.ParseException
import java.util.UUID

import com.github.j5ik2o.reactive.redis.CommandResponseParser.{ ArraySizeExpr, ErrorExpr, Expr, SimpleExpr }

import scala.util.parsing.input.Reader

object TransactionOperations {

  object MultiRequest extends SimpleResponseFactory {
    override def createResponseFromReader(requestId: UUID, message: Reader[Char]): (Response, Reader[Char]) =
      parseResponse(message) match {
        case (SimpleExpr(_), next) =>
          (MultiSucceeded(UUID.randomUUID(), requestId), next)
        case (ErrorExpr(msg), next) =>
          (MultiFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
        case (_, o) =>
          throw new ParseException("multi request", o.offset)
      }

    override protected val responseParser: Parser[Expr] = simpleWithCrLfOrErrorWithCrLf
  }

  case class MultiRequest(id: UUID) extends SimpleRequest {
    override val message: String = "MULTI"
    override val responseFactory: SimpleResponseFactory = MultiRequest
  }

  sealed trait MultiResponse extends Response

  case class MultiSucceeded(id: UUID, requestId: UUID) extends MultiResponse

  case class MultiFailed(id: UUID, requestId: UUID, ex: Exception) extends MultiResponse

  // ---

  object ExecRequest extends TransactionResponseFactory {
    override def createResponseFromReader(requestId: UUID, message: Reader[Char], responseFactories: Vector[SimpleResponseFactory]): (Response, Reader[Char]) = {
      val result = parseResponse(message)
      result match {
        case (ArraySizeExpr(size), next) =>
          val result: (Reader[Char], Seq[Response]) =
            if (size == -1)
              (next, Seq.empty)
            else
              responseFactories.foldLeft((next, Seq.empty[Response])) {
                case ((n, seq), e) =>
                  val (res, _n) = e.createResponseFromReader(requestId, n)
                  (_n, seq :+ res)
              }
          (ExecSucceeded(UUID.randomUUID(), requestId, result._2), result._1)
        case (ErrorExpr(msg), next) =>
          (ExecFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
        case (_, o) =>
          throw new ParseException("exec request", o.offset)

      }
    }

    override protected val responseParser: Parser[Expr] = arrayPrefixWithCrLfOrErrorWithCrLf

  }

  case class ExecRequest(id: UUID) extends TransactionRequest {
    override val message: String = "EXEC"
    override val responseFactory: TransactionResponseFactory = ExecRequest
  }

  sealed trait ExecResponse extends Response

  case class ExecSucceeded(id: UUID, requestId: UUID, responses: Seq[Response]) extends ExecResponse

  case class ExecFailed(id: UUID, requestId: UUID, ex: Exception) extends ExecResponse

  // ---

  object DiscardRequest extends SimpleResponseFactory {
    override def createResponseFromReader(requestId: UUID, message: Reader[Char]): (Response, Reader[Char]) =
      parseResponse(message) match {
        case (SimpleExpr(_), next) =>
          (DiscardSucceeded(UUID.randomUUID(), requestId), next)
        case (ErrorExpr(msg), next) =>
          (DiscardFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
        case (_, o) =>
          throw new ParseException("discard request", o.offset)
      }

    override protected val responseParser: Parser[Expr] = simpleWithCrLfOrErrorWithCrLf
  }

  case class DiscardRequest(id: UUID) extends SimpleRequest {
    override val message: String = "DISCARD"
    override val responseFactory: SimpleResponseFactory = DiscardRequest
  }

  sealed trait DiscardResponse extends Response

  case class DiscardSucceeded(id: UUID, requestId: UUID) extends DiscardResponse

  case class DiscardFailed(id: UUID, requestId: UUID, ex: Exception) extends DiscardResponse

  // ---

  object WatchRequest extends SimpleResponseFactory {
    override def createResponseFromReader(requestId: UUID, message: Reader[Char]): (Response, Reader[Char]) =
      parseResponse(message) match {
        case (SimpleExpr(_), next) =>
          (DiscardSucceeded(UUID.randomUUID(), requestId), next)
        case (ErrorExpr(msg), next) =>
          (DiscardFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
        case (_, o) =>
          throw new ParseException("watch request", o.offset)
      }

    override protected val responseParser: Parser[Expr] = simpleWithCrLfOrErrorWithCrLf
  }

  case class WatchRequest(id: UUID, key: String) extends SimpleRequest {
    override val message: String = s"WATCH $key"
    override val responseFactory: SimpleResponseFactory = WatchRequest
  }

  sealed trait WatchResponse extends Response

  case class WatchSucceeded(id: UUID, requestId: UUID) extends DiscardResponse

  case class WatchFailed(id: UUID, requestId: UUID, ex: Exception) extends DiscardResponse

}
