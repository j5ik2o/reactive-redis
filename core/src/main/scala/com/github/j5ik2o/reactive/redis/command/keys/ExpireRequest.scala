package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }

import scala.concurrent.duration.FiniteDuration

case class ExpireRequest(id: UUID, key: String, seconds: FiniteDuration)
    extends CommandRequest
    with StringParsersSupport {
  override type Response = ExpireResponse

  override def asString: String = s"EXPIRE $key ${seconds.toSeconds}"

  override protected def responseParser: P[Expr] = StringParsers.integerReply

  override protected def parseResponse: Handler = {
    case NumberExpr(n) =>
      ExpireSucceeded(UUID.randomUUID(), id, n == 1)
    case ErrorExpr(msg) =>
      ExpireFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }
}

sealed trait ExpireResponse                                              extends CommandResponse
case class ExpireSucceeded(id: UUID, requestId: UUID, isSet: Boolean)    extends ExpireResponse
case class ExpireFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends ExpireResponse
