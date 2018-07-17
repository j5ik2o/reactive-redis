package com.github.j5ik2o.reactive.redis.command.connection

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr, StringOptExpr }
import fastparse.all._

case class EchoRequest(id: UUID, message: String) extends CommandRequest with StringParsersSupport {

  override type Response = EchoResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"ECHO $message"

  override protected def responseParser: P[Expr] = P(bulkStringReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (StringOptExpr(message), next) =>
      (EchoSucceeded(UUID.randomUUID(), id, message.get), next)
    case (SimpleExpr(QUEUED), next) =>
      (EchoSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (EchoFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait EchoResponse                                              extends CommandResponse
case class EchoSuspended(id: UUID, requestId: UUID)                    extends EchoResponse
case class EchoSucceeded(id: UUID, requestId: UUID, message: String)   extends EchoResponse
case class EchoFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends EchoResponse
