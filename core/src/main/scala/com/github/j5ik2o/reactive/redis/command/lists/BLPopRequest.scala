package com.github.j5ik2o.reactive.redis.command.lists

import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model._
import fastparse.all._

import scala.concurrent.duration.Duration

final class BLPopRequest(val id: UUID, val keys: NonEmptyList[String], val timeout: Duration = Duration.Zero)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = BLPopResponse

  override val isMasterOnly: Boolean = true

  private def timeoutToSeconds: Long = if (timeout.isFinite()) timeout.toSeconds else 0

  override def asString: String = s"BLPOP ${keys.toList.mkString(" ")} $timeoutToSeconds"

  override protected lazy val responseParser: P[Expr] = fastParse(stringArrayReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (ArrayExpr(values), next) =>
      (BLPopSucceeded(UUID.randomUUID(), id, values.asInstanceOf[Seq[StringExpr]].map(_.value)), next)
    case (SimpleExpr(QUEUED), next) =>
      (BLPopSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (BLPopFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

object BLPopRequest {
  def apply(id: UUID, key: String, timeout: Duration): BLPopRequest =
    new BLPopRequest(id, NonEmptyList.one(key), timeout)
  def apply(id: UUID, keys: NonEmptyList[String], timeout: Duration): BLPopRequest = new BLPopRequest(id, keys, timeout)
}

sealed trait BLPopResponse                                                      extends CommandResponse
final case class BLPopSuspended(id: UUID, requestId: UUID)                      extends BLPopResponse
final case class BLPopSucceeded(id: UUID, requestId: UUID, values: Seq[String]) extends BLPopResponse
final case class BLPopFailed(id: UUID, requestId: UUID, ex: RedisIOException)   extends BLPopResponse
