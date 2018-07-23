package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model._
import fastparse.all._

case class MGetRequest(id: UUID, keys: Seq[String]) extends CommandRequest with StringParsersSupport {

  override type Response = MGetResponse

  override val isMasterOnly: Boolean = false

  override def asString: String = s"MGET ${keys.mkString(" ")}"

  override protected def responseParser: P[Expr] = P(stringArrayReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (ArrayExpr(values: Seq[StringExpr]), next) =>
      (MGetSucceeded(UUID.randomUUID(), id, values.map(_.value)), next)
    case (SimpleExpr(QUEUED), next) =>
      (MGetSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (MGetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait MGetResponse                                                extends CommandResponse
case class MGetSuspended(id: UUID, requestId: UUID)                      extends MGetResponse
case class MGetSucceeded(id: UUID, requestId: UUID, values: Seq[String]) extends MGetResponse
case class MGetFailed(id: UUID, requestId: UUID, ex: Exception)          extends MGetResponse
