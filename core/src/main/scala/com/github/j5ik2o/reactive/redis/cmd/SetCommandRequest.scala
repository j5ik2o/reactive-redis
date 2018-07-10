package com.github.j5ik2o.reactive.redis.cmd

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.model.{ ErrorExpr, Expr, SimpleExpr }
import com.github.j5ik2o.reactive.redis.parser.Parsers
import fastparse.all.P

case class SetCommandRequest(id: UUID, key: String, value: String) extends CommandRequest {
  override type Response = SetResponse

  override def asString: String = s"SET $key $value"

  protected def responseParser: P[Expr] = Parsers.simpleStringReply

  override def parseResponse: Handler = {
    case SimpleExpr("OK") =>
      SetSucceeded(UUID.randomUUID(), id)
    case ErrorExpr(msg) =>
      SetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }
}

sealed trait SetResponse extends CommandResponse

case class SetSucceeded(id: UUID, requestId: UUID) extends SetResponse

case class SetFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SetResponse
