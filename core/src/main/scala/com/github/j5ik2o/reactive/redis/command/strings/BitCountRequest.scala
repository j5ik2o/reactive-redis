package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command._
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

case class BitCountRequest(id: UUID, key: String, startAndEnd: Option[StartAndEnd] = None)
    extends SimpleCommandRequest
    with StringParsersSupport {
  override type Response = BitCountResponse

  override def asString: String = s"BITCOUNT $key" + startAndEnd.fold("")(e => " " + e.start + " " + e.end)

  override protected def responseParser: P[Expr] = P(StringParsers.integerReply | StringParsers.simpleStringReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (BitCountSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr("QUEUED"), next) =>
      (BitCountSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (BitCountFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }
}

sealed trait BitCountResponse                                              extends CommandResponse
case class BitCountSuspended(id: UUID, requestId: UUID)                    extends BitCountResponse
case class BitCountSucceeded(id: UUID, requestId: UUID, value: Int)        extends BitCountResponse
case class BitCountFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends BitCountResponse
