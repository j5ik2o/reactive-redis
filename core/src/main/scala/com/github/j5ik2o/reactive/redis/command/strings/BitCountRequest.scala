package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command._
import com.github.j5ik2o.reactive.redis.parser.Parsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }
import fastparse.all._

case class BitCountRequest(id: UUID, key: String, startAndEnd: Option[StartAndEnd] = None) extends CommandRequest {
  override type Response = BitCountResponse

  override def asString: String = s"BITCOUNT $key" + startAndEnd.fold("")(e => " " + e.start + " " + e.end)

  override protected def responseParser: P[Expr] = Parsers.integerReply

  override protected def parseResponse: Handler = {
    case NumberExpr(n) =>
      BitCountSucceeded(UUID.randomUUID(), id, n)
    case ErrorExpr(msg) =>
      BitCountFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }
}

sealed trait BitCountResponse extends CommandResponse

case class BitCountSucceeded(id: UUID, requestId: UUID, value: Int) extends BitCountResponse

case class BitCountFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends BitCountResponse
