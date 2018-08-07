package com.github.j5ik2o.reactive.redis.command.lists

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model._
import fastparse.all._

final case class LRangeRequest(id: UUID, key: String, start: Long, stop: Long)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = LRangeResponse
  override val isMasterOnly: Boolean = false

  override def asString: String = s"LRANGE $key $start $stop"

  override protected def responseParser: P[Expr] = fastParse(stringArrayReply | simpleStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (ArrayExpr(values), next) =>
      (LRangeSucceeded(UUID.randomUUID(), id, values.asInstanceOf[Seq[StringExpr]].map(_.value)), next)
    case (SimpleExpr(QUEUED), next) =>
      (LRangeSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (LRangeFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }
}

sealed trait LRangeResponse extends CommandResponse

final case class LRangeSucceeded(id: UUID, requestId: UUID, values: Seq[String]) extends LRangeResponse
final case class LRangeSuspended(id: UUID, requestId: UUID)                      extends LRangeResponse
final case class LRangeFailed(id: UUID, requestId: UUID, ex: RedisIOException)   extends LRangeResponse
