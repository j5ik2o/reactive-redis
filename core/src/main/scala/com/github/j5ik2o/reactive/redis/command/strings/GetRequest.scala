package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, StringOptExpr }

case class GetRequest(id: UUID, key: String) extends CommandRequest with StringParsersSupport {
  override type Response = GetResponse

  override def asString: String = s"GET $key"

  override protected def responseParser: P[Expr] = StringParsers.bulkStringWithCrLf

  override protected def parseResponse: Handler = {
    case StringOptExpr(s) =>
      GetSucceeded(UUID.randomUUID(), id, s)
    case ErrorExpr(msg) =>
      GetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }
}

trait GetResponse                                                         extends CommandResponse
case class GetSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends GetResponse
case class GetFailed(id: UUID, requestId: UUID, ex: RedisIOException)     extends GetResponse
