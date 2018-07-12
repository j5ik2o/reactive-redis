package com.github.j5ik2o.reactive.redis.command

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }
import com.github.j5ik2o.reactive.redis.parser.Parsers
import fastparse.all._

case class AppendRequest(id: UUID, key: String, value: String) extends CommandRequest {
  override type Response = AppendResponse

  override def asString: String = s"""APPEND $key "$value""""

  override protected def responseParser: P[Expr] = Parsers.integerReply

  override protected def parseResponse: Handler = {
    case NumberExpr(n) =>
      AppendSucceeded(UUID.randomUUID(), id, n)
    case ErrorExpr(msg) =>
      AppendFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }

}

sealed trait AppendResponse extends CommandResponse

case class AppendSucceeded(id: UUID, requestId: UUID, value: Int) extends AppendResponse

case class AppendFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends AppendResponse