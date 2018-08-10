package com.github.j5ik2o.reactive.redis.command.lists

import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model._
import fastparse.all._

import scala.concurrent.duration.Duration

final class BRPopRequest(val id: UUID, val keys: NonEmptyList[String], val timeout: Duration)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = BRPopResponse
  override val isMasterOnly: Boolean = true

  private def timetoutToSeconds: Long = if (timeout.isFinite()) timeout.toSeconds else 0

  override def asString: String = cs("BRPOP", keys.map(Some(_)).toList ++ List(Some(timetoutToSeconds.toString)): _*)

  override protected lazy val responseParser: P[Expr] = fastParse(stringArrayReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (ArrayExpr(values), next) =>
      (BRPopSucceeded(UUID.randomUUID(), id, values.asInstanceOf[Seq[StringExpr]].map(_.value)), next)
    case (SimpleExpr(QUEUED), next) =>
      (BRPopSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (BRPopFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }
}

object BRPopRequest {

  def apply(id: UUID, key: String, timeout: Duration): BRPopRequest =
    new BRPopRequest(id, NonEmptyList.one(key), timeout)

  def apply(id: UUID, keys: NonEmptyList[String], timeout: Duration): BRPopRequest = new BRPopRequest(id, keys, timeout)

}

sealed trait BRPopResponse extends CommandResponse

final case class BRPopSuspended(id: UUID, requestId: UUID) extends BRPopResponse

final case class BRPopSucceeded(id: UUID, requestId: UUID, values: Seq[String]) extends BRPopResponse

final case class BRPopFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends BRPopResponse
