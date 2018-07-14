package com.github.j5ik2o.reactive.redis.command.keys

import java.time.ZonedDateTime
import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }

case class ExpireAtRequest(id: UUID, key: String, expiresAt: ZonedDateTime)
    extends CommandRequest
    with StringParsersSupport {
  override type Response = ExpireAtResponse

  override def asString: String = s"""EXPIREAT $key ${expiresAt.toEpochSecond}"""

  override protected def responseParser: P[Expr] = StringParsers.integerReply

  override protected def parseResponse: Handler = {
    case NumberExpr(n) =>
      ExpireAtSucceeded(UUID.randomUUID(), id, n == 1)
    case ErrorExpr(msg) =>
      ExpireAtFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }
}

sealed trait ExpireAtResponse                                              extends CommandResponse
case class ExpireAtSucceeded(id: UUID, requestId: UUID, isSet: Boolean)    extends ExpireAtResponse
case class ExpireAtFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends ExpireAtResponse