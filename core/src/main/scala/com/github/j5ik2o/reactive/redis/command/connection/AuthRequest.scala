package com.github.j5ik2o.reactive.redis.command.connection

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import fastparse.all._

final case class AuthRequest(id: UUID, password: String) extends CommandRequest with StringParsersSupport {

  override type Response = AuthResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"AUTH $password"

  override protected def responseParser: P[Expr] = wrap(simpleStringReply)

  override protected def parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (AuthSucceeded(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (AuthFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait AuthResponse                                                    extends CommandResponse
final case class AuthSucceeded(id: UUID, requestId: UUID)                    extends AuthResponse
final case class AuthFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends AuthResponse
