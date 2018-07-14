package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, StringOptExpr }

case class GetRangeRequest(id: UUID, key: String, startAndEnd: StartAndEnd)
    extends CommandRequest
    with StringParsersSupport {
  override type Response = GetRangeResponse

  override def asString: String = s"GETRANGE $key ${startAndEnd.start} ${startAndEnd.end}"

  override protected def responseParser: P[Expr] = StringParsers.bulkStringReply

  override protected def parseResponse: Handler = {
    case StringOptExpr(s) =>
      GetRangeSucceeded(UUID.randomUUID(), id, s)
    case ErrorExpr(msg) =>
      GetRangeFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }
}

sealed trait GetRangeResponse                                                  extends CommandResponse
case class GetRangeSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends GetRangeResponse
case class GetRangeFailed(id: UUID, requestId: UUID, ex: RedisIOException)     extends GetRangeResponse
