package com.github.j5ik2o.reactive.redis.command.keys

import java.time.ZonedDateTime
import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class ExpireAtRequest(val id: UUID, val key: String, val expiresAt: ZonedDateTime)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = ExpireAtResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("EXPIREAT", Some(key), Some(expiresAt.toEpochSecond.toString))

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (ExpireAtSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (ExpireAtSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (ExpireAtFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: ExpireAtRequest =>
      id == that.id &&
      key == that.key &&
      expiresAt == that.expiresAt
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key, expiresAt)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"ExpireAtRequest($id, $key, $expiresAt)"
}

object ExpireAtRequest {

  def apply(id: UUID, key: String, expiresAt: ZonedDateTime): ExpireAtRequest = new ExpireAtRequest(id, key, expiresAt)

  def unapply(self: ExpireAtRequest): Option[(UUID, String, ZonedDateTime)] = Some((self.id, self.key, self.expiresAt))

  def create(id: UUID, key: String, expiresAt: ZonedDateTime): ExpireAtRequest = apply(id, key, expiresAt)

}

sealed trait ExpireAtResponse                                                    extends CommandResponse
final case class ExpireAtSuspended(id: UUID, requestId: UUID)                    extends ExpireAtResponse
final case class ExpireAtSucceeded(id: UUID, requestId: UUID, isSet: Boolean)    extends ExpireAtResponse
final case class ExpireAtFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends ExpireAtResponse
