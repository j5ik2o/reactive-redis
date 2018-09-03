package com.github.j5ik2o.reactive.redis.command.keys

import java.time.ZonedDateTime
import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class PExpireAtRequest(val id: UUID, val key: String, val millisecondsTimestamp: ZonedDateTime)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = PExpireAtResponse

  override val isMasterOnly: Boolean = true

  override def asString: String =
    cs("PEXPIREAT", Some(key), Some(millisecondsTimestamp.toInstant.toEpochMilli.toString))

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (PExpireAtSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (PExpireAtSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (PExpireAtFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: PExpireAtRequest =>
      id == that.id &&
      key == that.key &&
      millisecondsTimestamp == that.millisecondsTimestamp
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key, millisecondsTimestamp)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"PExpireAtRequest($id, $key, $millisecondsTimestamp)"

}

object PExpireAtRequest {

  def apply(id: UUID, key: String, millisecondsTimestamp: ZonedDateTime): PExpireAtRequest =
    new PExpireAtRequest(id, key, millisecondsTimestamp)

  def unapply(self: PExpireAtRequest): Option[(UUID, String, ZonedDateTime)] =
    Some((self.id, self.key, self.millisecondsTimestamp))

  def create(id: UUID, key: String, millisecondsTimestamp: ZonedDateTime): PExpireAtRequest =
    apply(id, key, millisecondsTimestamp)

}

sealed trait PExpireAtResponse                                                    extends CommandResponse
final case class PExpireAtSuspended(id: UUID, requestId: UUID)                    extends PExpireAtResponse
final case class PExpireAtSucceeded(id: UUID, requestId: UUID, isSet: Boolean)    extends PExpireAtResponse
final case class PExpireAtFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends PExpireAtResponse
