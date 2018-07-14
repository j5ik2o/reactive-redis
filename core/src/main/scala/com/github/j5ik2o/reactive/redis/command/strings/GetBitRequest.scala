package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }

case class GetBitRequest(id: UUID, key: String, offset: Int) extends CommandRequest with StringParsersSupport {
  override type Response = GetBitResponse

  override def asString: String = s"GETBIT $key $offset"

  override protected def responseParser: P[Expr] = StringParsers.integerReply

  override protected def parseResponse: Handler = {
    case NumberExpr(n) =>
      GetBitSucceeded(UUID.randomUUID(), id, n)
    case ErrorExpr(msg) =>
      GetBitFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }
}

sealed trait GetBitResponse                                              extends CommandResponse
case class GetBitSucceeded(id: UUID, requestId: UUID, value: Int)        extends GetBitResponse
case class GetBitFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends GetBitResponse
