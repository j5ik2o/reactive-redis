package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final case class TouchRequest(id: UUID, keys: NonEmptyList[String]) extends CommandRequest with StringParsersSupport {

  override type Response = TouchResponse
  override val isMasterOnly: Boolean = true

  override def asString: String = cs("TOUCH", keys.toList.map(Some(_)): _*)

  override protected def responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (TouchSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (TouchSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (TouchFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }
}

sealed trait TouchResponse                                                    extends CommandResponse
final case class TouchSucceeded(id: UUID, requestId: UUID, value: Long)       extends TouchResponse
final case class TouchSuspended(id: UUID, requestId: UUID)                    extends TouchResponse
final case class TouchFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends TouchResponse
