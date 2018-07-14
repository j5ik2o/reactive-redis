package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }

case class DelRequest(id: UUID, keys: NonEmptyList[String]) extends CommandRequest with StringParsersSupport {
  override type Response = DelResponse

  override def asString: String = s"DEL ${keys.toList.mkString(" ")}"

  override protected def responseParser: P[Expr] = StringParsers.integerReply

  override protected def parseResponse: Handler = {
    case NumberExpr(n) =>
      DelSucceeded(UUID.randomUUID(), id, n)
    case ErrorExpr(msg) =>
      DelFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }

}

sealed trait DelResponse                                              extends CommandResponse
case class DelSucceeded(id: UUID, requestId: UUID, value: Int)        extends DelResponse
case class DelFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends DelResponse
