package com.github.j5ik2o.reactive.redis.command.transactions

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }

final case class MultiRequest(id: UUID) extends CommandRequest with StringParsersSupport {

  override type Response = MultiResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = "MULTI"

  override protected def responseParser: P[Expr] = wrap(simpleStringReply)

  override protected def parseResponse: Handler = {
    case (SimpleExpr(_), next) =>
      (MultiSucceeded(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (MultiFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait MultiResponse                                                    extends CommandResponse
final case class MultiSucceeded(id: UUID, requestId: UUID)                    extends MultiResponse
final case class MultiFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends MultiResponse
