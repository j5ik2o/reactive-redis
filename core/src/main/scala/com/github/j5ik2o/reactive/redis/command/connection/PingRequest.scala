package com.github.j5ik2o.reactive.redis.command.connection

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model._
import fastparse.all._

final case class PingRequest(id: UUID, message: Option[String] = None)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = PingResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"PING${message.fold("")(v => s" $v")}"

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

}

sealed trait PingResponse                                                    extends CommandResponse
final case class PingSuspended(id: UUID, requestId: UUID)                    extends PingResponse
final case class PingSucceeded(id: UUID, requestId: UUID, message: String)   extends PingResponse
final case class PingFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends PingResponse
