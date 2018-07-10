package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command._
import com.github.j5ik2o.reactive.redis.parser.Parsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }
import fastparse.all._

case class BitPosRequest(id: UUID, key: String, bit: Int, startAndEnd: Option[StartAndEnd] = None)
    extends CommandRequest {
  override type Response = BitPosResponse

  override def asString: String = s"BITPOS $key $bit" + startAndEnd.fold("")(e => " " + e.start + " " + e.end)

  override protected def responseParser: P[Expr] = Parsers.integerReply

  override protected def parseResponse: Handler = {
    case NumberExpr(n) =>
      BitPosSucceeded(UUID.randomUUID, id, n)
    case ErrorExpr(msg) =>
      BitPosFailed(UUID.randomUUID, id, RedisIOException(Some(msg)))
  }
}

sealed trait BitPosResponse                                       extends CommandResponse
case class BitPosSucceeded(id: UUID, requestId: UUID, value: Int) extends BitPosResponse
case class BitPosFailed(id: UUID, requestId: UUID, ex: Exception) extends BitPosResponse
