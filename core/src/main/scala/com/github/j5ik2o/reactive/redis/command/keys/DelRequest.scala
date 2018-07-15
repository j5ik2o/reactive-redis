package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandResponse, SimpleCommandRequest, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }

case class DelRequest(id: UUID, keys: NonEmptyList[String]) extends SimpleCommandRequest with StringParsersSupport {
  override type Response = DelResponse

  override def asString: String = s"DEL ${keys.toList.mkString(" ")}"

  override protected def responseParser: P[Expr] = StringParsers.integerReply

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (DelSucceeded(UUID.randomUUID(), id, n), next)
    case (ErrorExpr(msg), next) =>
      (DelFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait DelResponse                                              extends CommandResponse
case class DelSucceeded(id: UUID, requestId: UUID, value: Int)        extends DelResponse
case class DelFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends DelResponse
