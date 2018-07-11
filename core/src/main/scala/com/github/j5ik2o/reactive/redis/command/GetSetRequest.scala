package com.github.j5ik2o.reactive.redis.command

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, StringOptExpr }
import com.github.j5ik2o.reactive.redis.parser.Parsers
import fastparse.all._

case class GetSetRequest(id: UUID, key: String, value: String) extends CommandRequest {
  override type Response = GetSetResponse

  override def asString: String = s"GETSET $key $value"

  override protected def responseParser: P[Expr] = Parsers.bulkStringReply | Parsers.simpleStringReply

  override protected def parseResponse: Handler = {
    case StringOptExpr(s) =>
      GetSetSucceeded(UUID.randomUUID(), id, s)
    case ErrorExpr(msg) =>
      GetSetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }
}

sealed trait GetSetResponse extends CommandResponse

case class GetSetSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends GetSetResponse

case class GetSetFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends GetSetResponse
