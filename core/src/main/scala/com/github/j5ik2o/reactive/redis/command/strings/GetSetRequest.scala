package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr, StringOptExpr }
import fastparse.all._

final case class GetSetRequest(id: UUID, key: String, value: String) extends CommandRequest with StringParsersSupport {

  override type Response = GetSetResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"GETSET $key $value"

  override protected def responseParser: P[Expr] = P(bulkStringReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (StringOptExpr(s), next) =>
      (GetSetSucceeded(UUID.randomUUID(), id, s), next)
    case (SimpleExpr(QUEUED), next) =>
      (GetSetSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (GetSetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait GetSetResponse                                                        extends CommandResponse
final case class GetSetSuspended(id: UUID, requestId: UUID)                        extends GetSetResponse
final case class GetSetSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends GetSetResponse
final case class GetSetFailed(id: UUID, requestId: UUID, ex: RedisIOException)     extends GetSetResponse
