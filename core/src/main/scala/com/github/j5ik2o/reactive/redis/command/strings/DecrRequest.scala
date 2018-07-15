package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandResponse, SimpleCommandRequest, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }

case class DecrRequest(id: UUID, key: String) extends SimpleCommandRequest with StringParsersSupport {
  override type Response = DecrResponse

  override def asString: String = s"DECR $key"

  override protected def responseParser: P[Expr] = StringParsers.integerReply

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (DecrSucceeded(UUID.randomUUID(), id, n), next)
    case (ErrorExpr(msg), next) =>
      (DecrFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }
}

sealed trait DecrResponse                                              extends CommandResponse
case class DecrSucceeded(id: UUID, requestId: UUID, value: Int)        extends DecrResponse
case class DecrFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends DecrResponse
