package com.github.j5ik2o.reactive.redis.command.lists

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model._
import fastparse.all._

final class LRangeRequest(val id: UUID, val key: String, val start: Long, val stop: Long)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = LRangeResponse
  override val isMasterOnly: Boolean = false

  override def asString: String = cs("LRANGE", Some(key), Some(start.toString), Some(stop.toString))

  override protected def responseParser: P[Expr] = fastParse(stringArrayReply | simpleStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (ArrayExpr(values), next) =>
      (LRangeSucceeded(UUID.randomUUID(), id, values.asInstanceOf[Seq[StringExpr]].map(_.value)), next)
    case (SimpleExpr(QUEUED), next) =>
      (LRangeSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (LRangeFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: LRangeRequest =>
      id == that.id &&
      key == that.key &&
      start == that.start &&
      stop == that.stop
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key, start, stop)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"LRangeRequest($id, $key, $start, $stop)"
}

object LRangeRequest {

  def apply(id: UUID, key: String, start: Long, stop: Long): LRangeRequest = new LRangeRequest(id, key, start, stop)

  def unapply(self: LRangeRequest): Option[(UUID, String, Long, Long)] =
    Some((self.id, self.key, self.start, self.stop))

  def create(id: UUID, key: String, start: Long, stop: Long): LRangeRequest = new LRangeRequest(id, key, start, stop)
}

sealed trait LRangeResponse extends CommandResponse

final case class LRangeSucceeded(id: UUID, requestId: UUID, values: Seq[String]) extends LRangeResponse
final case class LRangeSuspended(id: UUID, requestId: UUID)                      extends LRangeResponse
final case class LRangeFailed(id: UUID, requestId: UUID, ex: RedisIOException)   extends LRangeResponse
