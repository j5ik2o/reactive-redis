package com.github.j5ik2o.reactive.redis.command.connection

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }

final case class SelectRequest(id: UUID, index: Int) extends CommandRequest with StringParsersSupport {

  override type Response = SelectResponse
  override val isMasterOnly: Boolean = false

  override def asString: String = s"SELECT $index"

  override protected def responseParser: P[Expr] = fastParse(simpleStringReply)
  override protected def parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (SelectSucceeded(UUID.randomUUID(), id), next)
    case (SimpleExpr(QUEUED), next) =>
      (SelectSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (SelectFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }
}

sealed trait SelectResponse                                                    extends CommandResponse
final case class SelectSucceeded(id: UUID, requestId: UUID)                    extends SelectResponse
final case class SelectSuspended(id: UUID, requestId: UUID)                    extends SelectResponse
final case class SelectFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SelectResponse
