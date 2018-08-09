package com.github.j5ik2o.reactive.redis.command.connection

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import fastparse.all._

final case class SwapDBRequest(id: UUID, index0: Int, index1: Int) extends CommandRequest with StringParsersSupport {

  override type Response = SwapDBResponse
  override val isMasterOnly: Boolean = true

  override def asString: String = s"SWAPDB $index0 $index1"

  override protected def responseParser: P[Expr] = fastParse(simpleStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (SwapDBSucceeded(UUID.randomUUID(), id), next)
    case (SimpleExpr(QUEUED), next) =>
      (SwapDBSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (SwapDBFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }
}

sealed trait SwapDBResponse                                                    extends CommandResponse
final case class SwapDBSucceeded(id: UUID, requestId: UUID)                    extends SwapDBResponse
final case class SwapDBSuspended(id: UUID, requestId: UUID)                    extends SwapDBResponse
final case class SwapDBFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SwapDBResponse
