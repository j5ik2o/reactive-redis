package com.github.j5ik2o.reactive.redis.command.lists

import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model._
import fastparse.all._

import scala.concurrent.duration.Duration

final case class BLPopRequest(id: UUID, keys: NonEmptyList[String], timeout: Duration)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = BLPopResponse

  override val isMasterOnly: Boolean = true

  private def timetoutToSeconds: Long = if (timeout.isFinite()) timeout.toSeconds else 0

  override def asString: String = s"BLPOP ${keys.toList.mkString(" ")} ${timetoutToSeconds}"

  override protected def responseParser: P[Expr] = P(stringArrayReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (ArrayExpr(values), next) =>
      (BLPopSucceeded(UUID.randomUUID(), id, values.asInstanceOf[Seq[StringExpr]].map(_.value)), next)
    case (SimpleExpr(QUEUED), next) =>
      (BLPopSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (BLPopFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait BLPopResponse                                                      extends CommandResponse
final case class BLPopSuspended(id: UUID, requestId: UUID)                      extends BLPopResponse
final case class BLPopSucceeded(id: UUID, requestId: UUID, values: Seq[String]) extends BLPopResponse
final case class BLPopFailed(id: UUID, requestId: UUID, ex: RedisIOException)   extends BLPopResponse
