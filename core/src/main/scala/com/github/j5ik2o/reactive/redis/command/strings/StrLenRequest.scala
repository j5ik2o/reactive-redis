package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class StrLenRequest(val id: UUID, val key: String) extends CommandRequest with StringParsersSupport {

  override type Response = StrLenResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("STRLEN", Some(key))

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (StrLenSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (StrLenSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (StrLenFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: StrLenRequest =>
      id == that.id &&
      key == that.key
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"StrLenRequest($id, $key)"

}

object StrLenRequest {

  def apply(id: UUID, key: String): StrLenRequest = new StrLenRequest(id, key)

  def unapply(self: StrLenRequest): Option[(UUID, String)] = Some((self.id, self.key))

  def create(id: UUID, key: String): StrLenRequest = apply(id, key)

}

sealed trait StrLenResponse                                                    extends CommandResponse
final case class StrLenSuspended(id: UUID, requestId: UUID)                    extends StrLenResponse
final case class StrLenSucceeded(id: UUID, requestId: UUID, length: Long)      extends StrLenResponse
final case class StrLenFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends StrLenResponse
