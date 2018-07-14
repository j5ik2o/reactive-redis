package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }

case class SetBitRequest(id: UUID, key: String, offset: Int, value: Int)
    extends CommandRequest
    with StringParsersSupport {
  override type Response = SetBitResponse

  override def asString: String = s"SETBIT $key $offset $value"

  override protected def responseParser: P[Expr] = StringParsers.integerReply

  override protected def parseResponse: Handler = {
    case NumberExpr(n) =>
      SetBitSucceeded(UUID.randomUUID(), id, n)
    case ErrorExpr(msg) =>
      SetBitFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }

}

sealed trait SetBitResponse                                              extends CommandResponse
case class SetBitSucceeded(id: UUID, requestId: UUID, value: Int)        extends SetBitResponse
case class SetBitFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SetBitResponse
