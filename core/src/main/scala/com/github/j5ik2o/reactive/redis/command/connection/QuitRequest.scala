package com.github.j5ik2o.reactive.redis.command.connection

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }

final case class QuitRequest(id: UUID) extends CommandRequest with StringParsersSupport {

  override type Response = QuitResponse
  override val isMasterOnly: Boolean = false

  override def asString: String = "QUIT"

  override protected def responseParser: P[Expr] = fastParse(simpleStringReply)

  override protected def parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (QuitSucceeded(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (QuitFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait QuitResponse                                                    extends CommandResponse
final case class QuitSucceeded(id: UUID, requestId: UUID)                    extends QuitResponse
final case class QuitFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends QuitResponse
