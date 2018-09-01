package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class DecrByRequest(val id: UUID, val key: String, val value: Int)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = DecrByResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("DECRBY", Some(key), Some(value.toString))

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (DecrBySucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (DecrBySuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (DecrByFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: DecrByRequest =>
      id == that.id &&
      key == that.key &&
      value == that.value
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key, value)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"DecrByRequest($id, $key, $value)"
}

object DecrByRequest {

  def apply(id: UUID, key: String, value: Int): DecrByRequest = new DecrByRequest(id, key, value)

  def unapply(self: DecrByRequest): Option[(UUID, String, Int)] = Some((self.id, self.key, self.value))

  def create(id: UUID, key: String, value: Int): DecrByRequest = apply(id, key, value)

}

sealed trait DecrByResponse                                                    extends CommandResponse
final case class DecrBySuspended(id: UUID, requestId: UUID)                    extends DecrByResponse
final case class DecrBySucceeded(id: UUID, requestId: UUID, value: Long)       extends DecrByResponse
final case class DecrByFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends DecrByResponse
