package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr, StringOptExpr }
import fastparse.all._

final class GetRangeRequest(val id: UUID, val key: String, val startAndEnd: StartAndEnd)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = GetRangeResponse

  override val isMasterOnly: Boolean = false

  override def asString: String =
    cs("GETRANGE", Some(key), Some(startAndEnd.start.toString), Some(startAndEnd.end.toString))

  override protected lazy val responseParser: P[Expr] = fastParse(bulkStringReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (StringOptExpr(s), next) =>
      (GetRangeSucceeded(UUID.randomUUID(), id, s), next)
    case (SimpleExpr(QUEUED), next) =>
      (GetRangeSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (GetRangeFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: GetRangeRequest =>
      id == that.id &&
      key == that.key &&
      startAndEnd == that.startAndEnd
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key, startAndEnd)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"GetRangeRequest($id, $key, $startAndEnd)"

}

object GetRangeRequest {

  def apply(id: UUID, key: String, startAndEnd: StartAndEnd): GetRangeRequest =
    new GetRangeRequest(id, key, startAndEnd)

  def unapply(self: GetRangeRequest): Option[(UUID, String, StartAndEnd)] = Some((self.id, self.key, self.startAndEnd))

  def create(id: UUID, key: String, startAndEnd: StartAndEnd): GetRangeRequest =
    apply(id, key, startAndEnd)

}

sealed trait GetRangeResponse                                                        extends CommandResponse
final case class GetRangeSuspended(id: UUID, requestId: UUID)                        extends GetRangeResponse
final case class GetRangeSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends GetRangeResponse
final case class GetRangeFailed(id: UUID, requestId: UUID, ex: RedisIOException)     extends GetRangeResponse
