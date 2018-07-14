package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, StringOptExpr }

case class IncrByFloatRequest(id: UUID, key: String, value: Double) extends CommandRequest with StringParsersSupport {
  override type Response = IncrByFloatResponse

  override def asString: String = s"INCRBYFLOAT $key $value"

  override protected def responseParser: P[Expr] = StringParsers.bulkStringReply

  override protected def parseResponse: Handler = {
    case StringOptExpr(s) =>
      IncrByFloatSucceeded(UUID.randomUUID(), id, s.get.toDouble)
    case ErrorExpr(msg) =>
      IncrByFloatFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }

}

sealed trait IncrByFloatResponse                                              extends CommandResponse
case class IncrByFloatSucceeded(id: UUID, requestId: UUID, value: Double)     extends IncrByFloatResponse
case class IncrByFloatFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends IncrByFloatResponse
