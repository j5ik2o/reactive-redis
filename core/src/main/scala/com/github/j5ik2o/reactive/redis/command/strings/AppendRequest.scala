package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

case class AppendRequest(id: UUID, key: String, value: String) extends CommandRequest with StringParsersSupport {
  override val isMasterOnly: Boolean = true

  override type Response = AppendResponse

  override def asString: String = s"""APPEND $key "$value""""

  override protected def responseParser: P[Expr] = P(integerReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (AppendSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (AppendSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (AppendFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait AppendResponse                                              extends CommandResponse
case class AppendSuspended(id: UUID, requestId: UUID)                    extends AppendResponse
case class AppendSucceeded(id: UUID, requestId: UUID, value: Int)        extends AppendResponse
case class AppendFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends AppendResponse
