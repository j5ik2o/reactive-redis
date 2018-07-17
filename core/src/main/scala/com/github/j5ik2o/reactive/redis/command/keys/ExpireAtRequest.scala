package com.github.j5ik2o.reactive.redis.command.keys

import java.time.ZonedDateTime
import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

case class ExpireAtRequest(id: UUID, key: String, expiresAt: ZonedDateTime)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = ExpireAtResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"""EXPIREAT $key ${expiresAt.toEpochSecond}"""

  override protected def responseParser: P[Expr] = P(integerReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (ExpireAtSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (ExpireAtSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (ExpireAtFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait ExpireAtResponse                                              extends CommandResponse
case class ExpireAtSuspended(id: UUID, requestId: UUID)                    extends ExpireAtResponse
case class ExpireAtSucceeded(id: UUID, requestId: UUID, isSet: Boolean)    extends ExpireAtResponse
case class ExpireAtFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends ExpireAtResponse
