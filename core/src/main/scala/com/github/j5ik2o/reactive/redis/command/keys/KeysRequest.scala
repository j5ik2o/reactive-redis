package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model._
import fastparse.all._

case class KeysRequest(id: UUID, pattern: String) extends CommandRequest with StringParsersSupport {

  override type Response = KeysResponse

  override val isMasterOnly: Boolean = false

  override def asString: String = s"KEYS $pattern"

  override protected def responseParser: P[Expr] = P(stringOptArrayReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (ArrayExpr(values), next) =>
      val _values = values.asInstanceOf[Seq[StringOptExpr]]
      (KeysSucceeded(UUID.randomUUID(), id, _values.map(_.value.get)), next)
    case (SimpleExpr(QUEUED), next) =>
      (KeysSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (KeysFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait KeysResponse                                                extends CommandResponse
case class KeysSuspended(id: UUID, requestId: UUID)                      extends KeysResponse
case class KeysSucceeded(id: UUID, requestId: UUID, values: Seq[String]) extends KeysResponse
case class KeysFailed(id: UUID, requestId: UUID, ex: RedisIOException)   extends KeysResponse
