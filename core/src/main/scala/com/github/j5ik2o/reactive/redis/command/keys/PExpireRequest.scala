package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }

import scala.concurrent.duration.FiniteDuration

case class PExpireRequest(id: UUID, key: String, milliseconds: FiniteDuration)
    extends CommandRequest
    with StringParsersSupport {
  override type Response = PExpireResponse

  override def asString: String = s"PEXPIRE $key ${milliseconds.toMillis}"

  override protected def responseParser: P[Expr] = StringParsers.integerReply

  override protected def parseResponse: Handler = {
    case NumberExpr(n) =>
      PExpireSucceeded(UUID.randomUUID(), id, n == 1)
    case ErrorExpr(msg) =>
      PExpireFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }
}

sealed trait PExpireResponse                                              extends CommandResponse
case class PExpireSucceeded(id: UUID, requestId: UUID, isSet: Boolean)    extends PExpireResponse
case class PExpireFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends PExpireResponse
