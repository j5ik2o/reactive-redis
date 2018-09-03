package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import cats.Show
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class SetRangeRequest(val id: UUID, val key: String, val range: Int, val value: String)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = SetRangeResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("SETRANGE", Some(key), Some(range.toString), Some(value))

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (SetRangeSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (SetRangeSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (SetRangeFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: SetRangeRequest =>
      id == that.id &&
      key == that.key &&
      range == that.range &&
      value == that.value
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key, range, value)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"SetRangeRequest($id, $key, $range, $value)"

}

object SetRangeRequest {

  def apply[A](id: UUID, key: String, range: Int, value: A)(implicit s: Show[A]): SetRangeRequest =
    new SetRangeRequest(id, key, range, s.show(value))

  def unapply(self: SetRangeRequest): Option[(UUID, String, Int, String)] =
    Some((self.id, self.key, self.range, self.value))

  def create[A](id: UUID, key: String, range: Int, value: A, s: Show[A]): SetRangeRequest =
    new SetRangeRequest(id, key, range, s.show(value))

}

sealed trait SetRangeResponse                                                    extends CommandResponse
final case class SetRangeSuspended(id: UUID, requestId: UUID)                    extends SetRangeResponse
final case class SetRangeSucceeded(id: UUID, requestId: UUID, value: Long)       extends SetRangeResponse
final case class SetRangeFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SetRangeResponse
