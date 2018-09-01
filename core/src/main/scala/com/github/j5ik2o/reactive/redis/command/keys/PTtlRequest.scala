package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

import scala.concurrent.duration.Duration

final class PTtlRequest(val id: UUID, val key: String) extends CommandRequest with StringParsersSupport {

  override type Response = PTtlResponse
  override val isMasterOnly: Boolean = true

  override def asString: String = cs("PTTL", Some(key))

  override protected def responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (PTtlSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (PTtlSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (PTtlFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: PTtlRequest =>
      id == that.id &&
      key == that.key
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"PTtlRequest($id, $key)"
}

object PTtlRequest {

  def apply(id: UUID, key: String): PTtlRequest = new PTtlRequest(id, key)

  def unapply(self: PTtlRequest): Option[(UUID, String)] = Some((self.id, self.key))

  def create(id: UUID, key: String): PTtlRequest = apply(id, key)

}

sealed trait PTtlResponse extends CommandResponse

final case class PTtlSucceeded(id: UUID, requestId: UUID, value: Long) extends PTtlResponse {
  def toDuration: Duration = Duration(value, TimeUnit.MILLISECONDS)
}

final case class PTtlSuspended(id: UUID, requestId: UUID) extends PTtlResponse

final case class PTtlFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends PTtlResponse
