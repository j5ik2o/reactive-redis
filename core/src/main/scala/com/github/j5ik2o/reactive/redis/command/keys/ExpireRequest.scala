package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

import scala.concurrent.duration.FiniteDuration

case class ExpireRequest(id: UUID, key: String, seconds: FiniteDuration)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = ExpireResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"EXPIRE $key ${seconds.toSeconds}"

  override protected def responseParser: P[Expr] = P(integerReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (ExpireSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (ExpireSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (ExpireFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }
}

sealed trait ExpireResponse                                              extends CommandResponse
case class ExpireSuspended(id: UUID, requestId: UUID)                    extends ExpireResponse
case class ExpireSucceeded(id: UUID, requestId: UUID, isSet: Boolean)    extends ExpireResponse
case class ExpireFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends ExpireResponse
