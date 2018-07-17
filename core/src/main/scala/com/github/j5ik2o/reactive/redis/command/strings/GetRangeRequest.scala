package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr, StringOptExpr }
import fastparse.all._

case class GetRangeRequest(id: UUID, key: String, startAndEnd: StartAndEnd)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = GetRangeResponse

  override val isMasterOnly: Boolean = false

  override def asString: String = s"GETRANGE $key ${startAndEnd.start} ${startAndEnd.end}"

  override protected def responseParser: P[Expr] = P(bulkStringReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (StringOptExpr(s), next) =>
      (GetRangeSucceeded(UUID.randomUUID(), id, s), next)
    case (SimpleExpr(QUEUED), next) =>
      (GetRangeSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (GetRangeFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait GetRangeResponse                                                  extends CommandResponse
case class GetRangeSuspended(id: UUID, requestId: UUID)                        extends GetRangeResponse
case class GetRangeSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends GetRangeResponse
case class GetRangeFailed(id: UUID, requestId: UUID, ex: RedisIOException)     extends GetRangeResponse
