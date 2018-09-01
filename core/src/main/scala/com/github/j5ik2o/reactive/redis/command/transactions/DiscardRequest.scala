package com.github.j5ik2o.reactive.redis.command.transactions

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import fastparse.all._

final class DiscardRequest(val id: UUID) extends CommandRequest with StringParsersSupport {

  override type Response = DiscardResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = "DISCARD"

  override protected lazy val responseParser: P[Expr] = fastParse(simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (DiscardSucceeded(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (DiscardFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: DiscardRequest =>
      id == that.id
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"DiscardRequest($id)"

}

object DiscardRequest {

  def apply(id: UUID): DiscardRequest = new DiscardRequest(id)

  def unapply(self: DiscardRequest): Option[UUID] = Some(self.id)

  def create(id: UUID): DiscardRequest = apply(id)

}

sealed trait DiscardResponse                                                    extends CommandResponse
final case class DiscardSucceeded(id: UUID, requestId: UUID)                    extends DiscardResponse
final case class DiscardFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends DiscardResponse
