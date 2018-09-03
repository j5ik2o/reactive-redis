package com.github.j5ik2o.reactive.redis.command.connection

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model._
import fastparse.all._

final class PingRequest(val id: UUID, val message: Option[String]) extends CommandRequest with StringParsersSupport {
  require(!message.contains(""))

  override type Response = PingResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("PING", message)

  override protected lazy val responseParser: P[Expr] = fastParse(bulkStringReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (SimpleExpr(QUEUED), next) =>
      (PingSuspended(UUID.randomUUID(), id), next)
    case (SimpleExpr(message), next) =>
      (PingSucceeded(UUID.randomUUID(), id, message), next)
    case (StringOptExpr(message), next) =>
      (PingSucceeded(UUID.randomUUID(), id, message.getOrElse("")), next)
    case (ErrorExpr(msg), next) =>
      (PingFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: PingRequest =>
      id == that.id &&
      message == that.message
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, message)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"PingRequest($id, $message)"
}

object PingRequest {

  def apply(id: UUID, message: Option[String] = None): PingRequest = new PingRequest(id, message)

  def unapply(self: PingRequest): Option[(UUID, Option[String])] = Some((self.id, self.message))

  def create(id: UUID, message: Option[String] = None): PingRequest = apply(id, message)

}

sealed trait PingResponse                                                    extends CommandResponse
final case class PingSuspended(id: UUID, requestId: UUID)                    extends PingResponse
final case class PingSucceeded(id: UUID, requestId: UUID, message: String)   extends PingResponse
final case class PingFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends PingResponse
