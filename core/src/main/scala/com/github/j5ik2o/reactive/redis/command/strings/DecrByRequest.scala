package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandResponse, SimpleCommandRequest, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }

case class DecrByRequest(id: UUID, key: String, value: Int) extends SimpleCommandRequest with StringParsersSupport {
  override type Response = DecrByResponse

  override def asString: String = s"DECRBY $key $value"

  override protected def responseParser: P[Expr] = StringParsers.integerReply

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (DecrBySucceeded(UUID.randomUUID(), id, n), next)
    case (ErrorExpr(msg), next) =>
      (DecrByFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }
}

sealed trait DecrByResponse                                              extends CommandResponse
case class DecrBySucceeded(id: UUID, requestId: UUID, value: Int)        extends DecrByResponse
case class DecrByFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends DecrByResponse
