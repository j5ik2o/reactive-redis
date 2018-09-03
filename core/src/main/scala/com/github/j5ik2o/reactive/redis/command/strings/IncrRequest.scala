package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class IncrRequest(val id: UUID, val key: String) extends CommandRequest with StringParsersSupport {

  override type Response = IncrResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("INCR", Some(key))

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (IncrSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (IncrSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (IncrFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: IncrRequest =>
      id == that.id &&
      key == that.key
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"IncrRequest($id, $key)"
}

object IncrRequest {

  def apply(id: UUID, key: String): IncrRequest = new IncrRequest(id, key)

  def unapply(self: IncrRequest): Option[(UUID, String)] = Some((self.id, self.key))

  def create(id: UUID, key: String): IncrRequest = apply(id, key)

}

sealed trait IncrResponse                                                    extends CommandResponse
final case class IncrSuspended(id: UUID, requestId: UUID)                    extends IncrResponse
final case class IncrSucceeded(id: UUID, requestId: UUID, value: Long)       extends IncrResponse
final case class IncrFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends IncrResponse
