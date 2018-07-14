package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }

case class StrLenRequest(id: UUID, key: String) extends CommandRequest with StringParsersSupport {
  override type Response = StrLenResponse

  override def asString: String = s"STRLEN $key"

  override protected def responseParser: P[Expr] = StringParsers.integerReply

  override protected def parseResponse: Handler = {
    case NumberExpr(n) =>
      StrLenSucceeded(UUID.randomUUID(), id, n)
    case ErrorExpr(msg) =>
      StrLenFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }

}

sealed trait StrLenResponse                                              extends CommandResponse
case class StrLenSucceeded(id: UUID, requestId: UUID, length: Int)       extends StrLenResponse
case class StrLenFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends StrLenResponse