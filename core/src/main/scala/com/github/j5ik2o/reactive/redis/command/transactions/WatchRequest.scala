package com.github.j5ik2o.reactive.redis.command.transactions

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import fastparse.all._

final case class WatchRequest(id: UUID, keys: Set[String]) extends CommandRequest with StringParsersSupport {

  override type Response = WatchResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"WATCH ${keys.mkString(" ")}"

  override protected lazy val responseParser: P[Expr] = fastParse(simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (SimpleExpr(QUEUED), next) =>
      (WatchSuspended(UUID.randomUUID(), id), next)
    case (SimpleExpr(OK), next) =>
      (WatchSucceeded(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (WatchFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait WatchResponse                                                    extends CommandResponse
final case class WatchSucceeded(id: UUID, requestId: UUID)                    extends WatchResponse
final case class WatchSuspended(id: UUID, requestId: UUID)                    extends WatchResponse
final case class WatchFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends WatchResponse
