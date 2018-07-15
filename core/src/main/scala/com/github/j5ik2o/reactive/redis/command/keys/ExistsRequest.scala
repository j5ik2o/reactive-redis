package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandResponse, SimpleCommandRequest, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }

case class ExistsRequest(id: UUID, keys: NonEmptyList[String]) extends SimpleCommandRequest with StringParsersSupport {
  override type Response = ExistsResponse

  override def asString: String = s"EXISTS ${keys.toList.mkString(" ")}"

  override protected def responseParser: P[Expr] = StringParsers.integerReply

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (ExistsSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (ErrorExpr(msg), next) =>
      (ExistsFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait ExistsResponse                                              extends CommandResponse
case class ExistsSucceeded(id: UUID, requestId: UUID, isExists: Boolean) extends ExistsResponse
case class ExistsFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends ExistsResponse
