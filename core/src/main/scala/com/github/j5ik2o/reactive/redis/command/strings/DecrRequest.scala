package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse }
import com.github.j5ik2o.reactive.redis.parser.Parsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }
import fastparse.all._

case class DecrRequest(id: UUID, key: String) extends CommandRequest {
  override type Response = DecrResponse

  override def asString: String = s"DECR $key"

  override protected def responseParser: P[Expr] = Parsers.integerReply

  override protected def parseResponse: Handler = {
    case NumberExpr(n) =>
      DecrSucceeded(UUID.randomUUID(), id, n)
    case ErrorExpr(msg) =>
      DecrFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }
}

sealed trait DecrResponse                                              extends CommandResponse
case class DecrSucceeded(id: UUID, requestId: UUID, value: Int)        extends DecrResponse
case class DecrFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends DecrResponse
