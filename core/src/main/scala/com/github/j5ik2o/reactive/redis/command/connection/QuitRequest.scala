package com.github.j5ik2o.reactive.redis.command.connection

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }

final class QuitRequest(val id: UUID) extends CommandRequest with StringParsersSupport {

  override type Response = QuitResponse
  override val isMasterOnly: Boolean = false

  override def asString: String = cs("QUIT")

  override protected def responseParser: P[Expr] = fastParse(simpleStringReply)

  override protected def parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (QuitSucceeded(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (QuitFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: QuitRequest =>
      id == that.id
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"QuitRequest($id)"

}

object QuitRequest {

  def apply(id: UUID): QuitRequest = new QuitRequest(id)

  def create(id: UUID): QuitRequest = apply(id)

}

sealed trait QuitResponse                                                    extends CommandResponse
final case class QuitSucceeded(id: UUID, requestId: UUID)                    extends QuitResponse
final case class QuitFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends QuitResponse
