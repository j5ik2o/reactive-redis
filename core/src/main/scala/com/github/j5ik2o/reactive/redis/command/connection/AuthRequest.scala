package com.github.j5ik2o.reactive.redis.command.connection

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import fastparse.all._

final class AuthRequest(val id: UUID, val password: String) extends CommandRequest with StringParsersSupport {

  override type Response = AuthResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("AUTH", Some(password))

  override protected lazy val responseParser: P[Expr] = fastParse(simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (AuthSucceeded(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (AuthFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: AuthRequest =>
      id == that.id &&
      password == that.password
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, password)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"AuthRequest($id, *****)"

}

object AuthRequest {

  def apply(id: UUID, password: String): AuthRequest = new AuthRequest(id, password)

  def unapply(self: AuthRequest): Option[(UUID, String)] = Some((self.id, self.password))

  def create(id: UUID, password: String): AuthRequest = apply(id, password)

}

sealed trait AuthResponse                                                    extends CommandResponse
final case class AuthSucceeded(id: UUID, requestId: UUID)                    extends AuthResponse
final case class AuthFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends AuthResponse
