package com.github.j5ik2o.reactive.redis.command.keys

import java.time.ZonedDateTime
import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final case class PExpireAtRequest(id: UUID, key: String, millisecondsTimestamp: ZonedDateTime)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = PExpireAtResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"PEXPIREAT $key ${millisecondsTimestamp.toInstant.toEpochMilli}"

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (PExpireAtSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (PExpireAtSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (PExpireAtFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }
}

sealed trait PExpireAtResponse                                                    extends CommandResponse
final case class PExpireAtSuspended(id: UUID, requestId: UUID)                    extends PExpireAtResponse
final case class PExpireAtSucceeded(id: UUID, requestId: UUID, isSet: Boolean)    extends PExpireAtResponse
final case class PExpireAtFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends PExpireAtResponse
