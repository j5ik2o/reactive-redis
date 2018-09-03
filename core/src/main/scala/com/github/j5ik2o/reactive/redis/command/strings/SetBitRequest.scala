package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class SetBitRequest(val id: UUID, val key: String, val offset: Long, val value: Long)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = SetBitResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("SETBIT", Some(key), Some(offset.toString), Some(value.toString))

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (SetBitSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (SetBitSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (SetBitFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: SetBitRequest =>
      id == that.id &&
      key == that.key &&
      offset == that.offset &&
      value == that.value
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(id, key, offset, value)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"SetBitRequest($id, $key, $offset, $value)"
}

object SetBitRequest {

  def apply(id: UUID, key: String, offset: Long, value: Long): SetBitRequest = new SetBitRequest(id, key, offset, value)

  def unapply(self: SetBitRequest): Option[(UUID, String, Long, Long)] =
    Some((self.id, self.key, self.offset, self.value))

  def create(id: UUID, key: String, offset: Long, value: Long): SetBitRequest = apply(id, key, offset, value)

}

sealed trait SetBitResponse                                                    extends CommandResponse
final case class SetBitSuspended(id: UUID, requestId: UUID)                    extends SetBitResponse
final case class SetBitSucceeded(id: UUID, requestId: UUID, value: Long)       extends SetBitResponse
final case class SetBitFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SetBitResponse
