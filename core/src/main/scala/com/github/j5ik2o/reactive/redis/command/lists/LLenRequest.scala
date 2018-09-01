package com.github.j5ik2o.reactive.redis.command.lists
import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class LLenRequest(val id: UUID, val key: String) extends CommandRequest with StringParsersSupport {

  override type Response = LLenResponse
  override val isMasterOnly: Boolean = false

  override def asString: String = cs("LLEN", Some(key))

  override protected def responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (LLenSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (LLenSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (LLenFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: LLenRequest =>
      id == that.id &&
      key == that.key
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"LLenRequest($id, $key)"
}

object LLenRequest {

  def apply(id: UUID, key: String): LLenRequest = new LLenRequest(id, key)

  def unapply(self: LLenRequest): Option[(UUID, String)] = Some((self.id, self.key))

  def create(id: UUID, key: String): LLenRequest = apply(id, key)

}

sealed trait LLenResponse                                                    extends CommandResponse
final case class LLenSucceeded(id: UUID, requestId: UUID, value: Long)       extends LLenResponse
final case class LLenSuspended(id: UUID, requestId: UUID)                    extends LLenResponse
final case class LLenFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends LLenResponse
