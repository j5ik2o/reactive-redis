package com.github.j5ik2o.reactive.redis.command.hashes

import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final case class HDelRequest(id: UUID, key: String, fields: NonEmptyList[String])
    extends CommandRequest
    with StringParsersSupport {

  override type Response = HDelResponse
  override val isMasterOnly: Boolean = true

  override def asString: String = s"HDEL $key ${fields.toList.mkString(" ")}"

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (HDelSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (HDelSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (HDelFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait HDelResponse                                                      extends CommandResponse
final case class HDelSuspended(id: UUID, requestId: UUID)                      extends HDelResponse
final case class HDelSucceeded(id: UUID, requestId: UUID, numberDeleted: Long) extends HDelResponse
final case class HDelFailed(id: UUID, requestId: UUID, ex: RedisIOException)   extends HDelResponse
