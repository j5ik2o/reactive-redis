package com.github.j5ik2o.reactive.redis.command

import java.text.ParseException
import java.util.UUID

import com.github.j5ik2o.reactive.redis.parser.model.Expr
import fastparse.core
import fastparse.core.Parsed
import scodec.bits.ByteVector

trait CommandRequest {
  type Elem
  type Repr
  type P[+T] = core.Parser[T, Elem, Repr]

  type Response <: CommandResponse
  type Handler = PartialFunction[Expr, Response]

  val id: UUID
  def asString: String

  protected def responseParser: P[Expr]

  protected def convertToParseSource(s: ByteVector): Repr

  def parse(text: ByteVector): Either[ParseException, Response] = {
    responseParser.parse(convertToParseSource(text)) match {
      case f @ Parsed.Failure(_, index, _) =>
        Left(new ParseException(f.msg, index))
      case Parsed.Success(value, _) => Right(parseResponse(value))
    }
  }

  protected def parseResponse: Handler
}
