package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

import scala.concurrent.duration.Duration

final class TtlRequest(val id: UUID, val key: String) extends CommandRequest with StringParsersSupport {

  override type Response = TtlResponse
  override val isMasterOnly: Boolean = true

  override def asString: String = cs("TTL", Some(key))

  override protected def responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (TtlSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (TtlSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (TtlFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: TtlRequest =>
      id == that.id &&
      key == that.key
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"TtlRequest($id, $key)"

}

object TtlRequest {

  def apply(id: UUID, key: String): TtlRequest = new TtlRequest(id, key)

  def unapply(self: TtlRequest): Option[(UUID, String)] = Some((self.id, self.key))

  def create(id: UUID, key: String): TtlRequest = apply(id, key)

}

sealed trait TtlResponse extends CommandResponse

final case class TtlSucceeded(id: UUID, requestId: UUID, value: Long) extends TtlResponse {
  def toDuration: Duration = Duration(value, TimeUnit.SECONDS)
}

final case class TtlSuspended(id: UUID, requestId: UUID) extends TtlResponse

final case class TtlFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends TtlResponse
