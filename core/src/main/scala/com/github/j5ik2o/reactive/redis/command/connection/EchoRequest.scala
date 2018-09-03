package com.github.j5ik2o.reactive.redis.command.connection

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr, StringOptExpr }
import fastparse.all._

final class EchoRequest(val id: UUID, val message: String) extends CommandRequest with StringParsersSupport {

  override type Response = EchoResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("ECHO", Some(message))

  override protected lazy val responseParser: P[Expr] = fastParse(bulkStringReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (StringOptExpr(message), next) =>
      (EchoSucceeded(UUID.randomUUID(), id, message.getOrElse("")), next)
    case (SimpleExpr(QUEUED), next) =>
      (EchoSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (EchoFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: EchoRequest =>
      id == that.id &&
      message == that.message
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, message)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"EchoRequest($id, $message)"

}

object EchoRequest {

  def apply(id: UUID, message: String): EchoRequest = new EchoRequest(id, message)

  def unapply(self: EchoRequest): Option[(UUID, String)] = Some((self.id, self.message))

  def create(id: UUID, message: String): EchoRequest = apply(id, message)

}

sealed trait EchoResponse                                                    extends CommandResponse
final case class EchoSuspended(id: UUID, requestId: UUID)                    extends EchoResponse
final case class EchoSucceeded(id: UUID, requestId: UUID, message: String)   extends EchoResponse
final case class EchoFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends EchoResponse
