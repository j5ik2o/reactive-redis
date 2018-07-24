package com.github.j5ik2o.reactive.redis.command.server

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import fastparse.all._

final case class FlushAllRequest(id: UUID, async: Boolean = false) extends CommandRequest with StringParsersSupport {

  override type Response = FlushAllResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"FLUSHALL" + (if (async) " ASYNC" else "")

  override protected def responseParser: P[Expr] = P(simpleStringReply)

  override protected def parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (FlushAllSucceeded(UUID.randomUUID(), id), next)
    case (SimpleExpr(QUEUED), next) =>
      (FlushAllSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (FlushAllFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait FlushAllResponse                                                    extends CommandResponse
final case class FlushAllSuspended(id: UUID, requestId: UUID)                    extends FlushAllResponse
final case class FlushAllSucceeded(id: UUID, requestId: UUID)                    extends FlushAllResponse
final case class FlushAllFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends FlushAllResponse
