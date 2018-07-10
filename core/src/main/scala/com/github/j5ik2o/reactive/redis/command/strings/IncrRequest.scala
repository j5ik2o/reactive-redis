package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse }
import com.github.j5ik2o.reactive.redis.parser.Parsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }
import fastparse.all._

case class IncrRequest(id: UUID, key: String) extends CommandRequest {
  override type Response = IncrResponse

  override def asString: String = s"INCR $key"

  override protected def responseParser: P[Expr] = Parsers.integerReply

  override protected def parseResponse: Handler = {
    case NumberExpr(n) =>
      IncrSucceeded(UUID.randomUUID(), id, n)
    case ErrorExpr(msg) =>
      IncrFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }

}

sealed trait IncrResponse                                              extends CommandResponse
case class IncrSucceeded(id: UUID, requestId: UUID, value: Int)        extends IncrResponse
case class IncrFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends IncrResponse
