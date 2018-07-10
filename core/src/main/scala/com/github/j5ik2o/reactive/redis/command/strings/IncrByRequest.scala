package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse }
import com.github.j5ik2o.reactive.redis.parser.Parsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }
import fastparse.all._

case class IncrByRequest(id: UUID, key: String, value: Int) extends CommandRequest {
  override type Response = IncrByResponse

  override def asString: String = s"INCRBY $key $value"

  override protected def responseParser: P[Expr] = Parsers.integerReply

  override protected def parseResponse: Handler = {
    case NumberExpr(n) =>
      IncrBySucceeded(UUID.randomUUID(), id, n)
    case ErrorExpr(msg) =>
      IncrByFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }
}

sealed trait IncrByResponse                                              extends CommandResponse
case class IncrBySucceeded(id: UUID, requestId: UUID, value: Int)        extends IncrByResponse
case class IncrByFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends IncrByResponse
