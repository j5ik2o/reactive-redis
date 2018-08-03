package com.github.j5ik2o.reactive.redis.command.transactions

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import fastparse.all._

final case class UnwatchRequest(id: UUID) extends CommandRequest with StringParsersSupport {

  override type Response = UnwatchResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = "UNWATCH"

  override protected lazy val responseParser: P[Expr] = fastParse(simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (UnwatchSucceeded(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (UnwatchFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait UnwatchResponse                                                    extends CommandResponse
final case class UnwatchSucceeded(id: UUID, requestId: UUID)                    extends UnwatchResponse
final case class UnwatchFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends UnwatchResponse
