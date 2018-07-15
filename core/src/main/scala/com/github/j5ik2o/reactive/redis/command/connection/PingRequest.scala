package com.github.j5ik2o.reactive.redis.command.connection

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandResponse, SimpleCommandRequest, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, StringOptExpr }

case class PingRequest(id: UUID, message: Option[String] = None)
    extends SimpleCommandRequest
    with StringParsersSupport {
  override type Response = PingResponse

  override def asString: String = s"PING ${message.getOrElse("")}"

  override protected def responseParser: P[Expr] = StringParsers.bulkStringWithCrLf

  override protected def parseResponse: Handler = {
    case (StringOptExpr(message), next) => (PingSucceeded(UUID.randomUUID(), id, message.get), next)
    case (ErrorExpr(msg), next)         => (PingFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }
}

sealed trait PingResponse                                              extends CommandResponse
case class PingSucceeded(id: UUID, requestId: UUID, message: String)   extends PingResponse
case class PingFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends PingResponse
