package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final case class DelRequest(id: UUID, keys: NonEmptyList[String]) extends CommandRequest with StringParsersSupport {

  override type Response = DelResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"DEL ${keys.toList.mkString(" ")}"

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (DelSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (DelSuspended(id, id), next)
    case (ErrorExpr(msg), next) =>
      (DelFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait DelResponse                                                    extends CommandResponse
final case class DelSuspended(id: UUID, requestId: UUID)                    extends DelResponse
final case class DelSucceeded(id: UUID, requestId: UUID, value: Long)       extends DelResponse
final case class DelFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends DelResponse
