package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command._
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }

case class BitPosRequest(id: UUID, key: String, bit: Int, startAndEnd: Option[StartAndEnd] = None)
    extends SimpleCommandRequest
    with StringParsersSupport {
  override type Response = BitPosResponse

  override def asString: String = s"BITPOS $key $bit" + startAndEnd.fold("")(e => " " + e.start + " " + e.end)

  override protected def responseParser: P[Expr] = StringParsers.integerReply

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (BitPosSucceeded(UUID.randomUUID, id, n), next)
    case (ErrorExpr(msg), next) =>
      (BitPosFailed(UUID.randomUUID, id, RedisIOException(Some(msg))), next)
  }
}

sealed trait BitPosResponse                                       extends CommandResponse
case class BitPosSucceeded(id: UUID, requestId: UUID, value: Int) extends BitPosResponse
case class BitPosFailed(id: UUID, requestId: UUID, ex: Exception) extends BitPosResponse
