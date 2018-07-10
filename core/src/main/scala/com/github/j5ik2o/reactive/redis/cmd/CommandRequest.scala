package com.github.j5ik2o.reactive.redis.cmd

import java.text.ParseException
import java.util.UUID

import com.github.j5ik2o.reactive.redis.model.Expr
import fastparse.all._
import fastparse.core.Parsed

trait CommandRequest {
  type Response <: CommandResponse
  type Handler = PartialFunction[Expr, Response]
  val id: UUID
  def asString: String
  protected def responseParser: P[Expr]

  def parse(text: String): Either[ParseException, Response] = {
    responseParser.parse(text) match {
      case f @ Parsed.Failure(_, index, _) =>
        Left(new ParseException(f.msg, index))
      case Parsed.Success(value, _) => Right(parseResponse(value))
    }
  }

  protected def parseResponse: Handler
}
