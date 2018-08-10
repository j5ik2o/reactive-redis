package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

import scala.concurrent.duration.FiniteDuration

final case class PExpireRequest(id: UUID, key: String, milliseconds: FiniteDuration)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = PExpireResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("PEXPIRE", Some(key), Some(milliseconds.toMillis.toString))

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (PExpireSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (PExpireSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (PExpireFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }
}

sealed trait PExpireResponse                                                    extends CommandResponse
final case class PExpireSuspended(id: UUID, requestId: UUID)                    extends PExpireResponse
final case class PExpireSucceeded(id: UUID, requestId: UUID, isSet: Boolean)    extends PExpireResponse
final case class PExpireFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends PExpireResponse
