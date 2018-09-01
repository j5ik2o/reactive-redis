package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class DecrRequest(val id: UUID, val key: String) extends CommandRequest with StringParsersSupport {

  override type Response = DecrResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("DECR", Some(key))

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (DecrSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (DecrSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (DecrFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: DecrRequest =>
      id == that.id &&
      key == that.key
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"DecrRequest($id, $key)"

}

object DecrRequest {

  def apply(id: UUID, key: String): DecrRequest = new DecrRequest(id, key)

  def unapply(self: DecrRequest): Option[(UUID, String)] = Some((self.id, self.key))

  def create(id: UUID, key: String): DecrRequest = apply(id, key)

}

sealed trait DecrResponse                                                    extends CommandResponse
final case class DecrSuspended(id: UUID, requestId: UUID)                    extends DecrResponse
final case class DecrSucceeded(id: UUID, requestId: UUID, value: Long)       extends DecrResponse
final case class DecrFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends DecrResponse
