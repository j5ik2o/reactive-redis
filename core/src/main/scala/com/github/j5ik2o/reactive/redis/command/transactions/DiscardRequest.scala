package com.github.j5ik2o.reactive.redis.command.transactions

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }

final case class DiscardRequest(id: UUID) extends CommandRequest with StringParsersSupport {

  override type Response = DiscardResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = "DISCARD"

  override protected def responseParser: P[Expr] = simpleStringReply

  override protected def parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (DiscardSucceeded(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (DiscardFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait DiscardResponse                                                    extends CommandResponse
final case class DiscardSucceeded(id: UUID, requestId: UUID)                    extends DiscardResponse
final case class DiscardFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends DiscardResponse
