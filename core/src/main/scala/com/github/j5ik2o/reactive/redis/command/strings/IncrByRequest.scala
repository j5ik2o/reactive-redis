package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

@SuppressWarnings(Array("org.wartremover.warts.ToString"))
final class IncrByRequest(val id: UUID, val key: String, val value: Int)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = IncrByResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("INCRBY", Some(key), Some(value.toString))

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (IncrBySucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (IncrBySuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (IncrByFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: IncrByRequest =>
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

  override def toString: String = s"IncrByRequest($id, $key, $value)"

}

object IncrByRequest {

  def apply(id: UUID, key: String, value: Int): IncrByRequest = new IncrByRequest(id, key, value)

  def unapply(self: IncrByRequest): Option[(UUID, String, Int)] = Some((self.id, self.key, self.value))

  def create(id: UUID, key: String, value: Int): IncrByRequest = apply(id, key, value)

}

sealed trait IncrByResponse                                                    extends CommandResponse
final case class IncrBySuspended(id: UUID, requestId: UUID)                    extends IncrByResponse
final case class IncrBySucceeded(id: UUID, requestId: UUID, value: Long)       extends IncrByResponse
final case class IncrByFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends IncrByResponse
