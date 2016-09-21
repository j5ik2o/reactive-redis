package com.github.j5ik2o.reactive.redis

import com.github.j5ik2o.reactive.redis.CommandResponseParser._

import scala.util.parsing.combinator.RegexParsers

object BaseProtocol extends BaseProtocol

trait BaseProtocol {

  trait CommandRequest extends RegexParsers {

    def encodeAsString: String

    override def toString: String = encodeAsString

    type ResultType

    type ResponseType <: CommandResponse

    def responseAsSucceeded(arguments: ResultType): ResponseType

    def responseAsFailed(ex: Exception): ResponseType

    val parser: CommandResponseParser[ResponseType]

    protected class SimpleReplyParser(
      responseAsSucceeded: String => ResponseType,
      responseAsFailed:    String => ResponseType
    )
        extends CommandResponseParser[ResponseType] {
      override protected val responseParser: Parser[ResponseType] = {
        simpleWithCrLfOrErrorWithCrLf ^^ {
          case SimpleExpr(msg) =>
            responseAsSucceeded(msg)
          case ErrorExpr(msg) =>
            responseAsFailed(msg)
          case _ =>
            sys.error("It's unexpected.")
        }
      }
    }

    protected class UnitReplyParser(
      responseAsSucceeded: Unit => ResponseType,
      responseAsFailed:    String => ResponseType
    )
        extends CommandResponseParser[ResponseType] {
      override protected val responseParser: Parser[ResponseType] = {
        simpleWithCrLfOrErrorWithCrLf ^^ {
          case SimpleExpr(_) =>
            responseAsSucceeded(())
          case ErrorExpr(msg) =>
            responseAsFailed(msg)
          case _ =>
            sys.error("It's unexpected.")
        }
      }
    }

    protected class BulkStringParser(
      responseAsSucceeded: (Option[String]) => ResponseType,
      responseAsFailed:    String => ResponseType
    )
        extends CommandResponseParser[ResponseType] {
      override protected val responseParser: Parser[ResponseType] = {
        bulkStringWithCrLfOrErrorWithCrLf ^^ {
          case StringOptExpr(msg) =>
            responseAsSucceeded(msg)
          case ErrorExpr(msg) =>
            responseAsFailed(msg)
          case _ =>
            sys.error("It's unexpected.")
        }
      }
    }

    protected class IntegerReplyParser(
      responseAsSucceeded: (Int) => ResponseType,
      responseAsFailed:    String => ResponseType
    )
        extends CommandResponseParser[ResponseType] {
      override protected val responseParser: Parser[ResponseType] = {
        integerReplyWithCrLfOrErrorWithCrLf ^^ {
          case NumberExpr(n) =>
            responseAsSucceeded(n)
          case ErrorExpr(msg) =>
            responseAsFailed(msg)
          case _ =>
            sys.error("It's unexpected.")
        }
      }
    }

    protected class StringArrayParser(
      responseAsSucceeded: (Seq[String]) => ResponseType,
      responseAsFailed:    String => ResponseType
    )
        extends CommandResponseParser[ResponseType] {
      override protected val responseParser: Parser[ResponseType] = {
        stringArrayWithCrLfOrErrorWithCrLf ^^ {
          case ArrayExpr(values) =>
            responseAsSucceeded(values.asInstanceOf[Seq[StringExpr]].map(_.value))
          case ErrorExpr(msg) =>
            responseAsFailed(msg)
          case _ =>
            sys.error("It's unexpected.")
        }
      }
    }

  }

  trait CommandResponse

}
