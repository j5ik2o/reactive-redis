package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandResponse, SimpleCommandRequest, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, StringOptExpr }

case class GetSetRequest(id: UUID, key: String, value: String) extends SimpleCommandRequest with StringParsersSupport {
  override type Response = GetSetResponse

  override def asString: String = s"GETSET $key $value"

  override protected def responseParser: P[Expr] = StringParsers.bulkStringReply

  override protected def parseResponse: Handler = {
    case (StringOptExpr(s), next) =>
      (GetSetSucceeded(UUID.randomUUID(), id, s), next)
    case (ErrorExpr(msg), next) =>
      (GetSetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }
}

sealed trait GetSetResponse                                                  extends CommandResponse
case class GetSetSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends GetSetResponse
case class GetSetFailed(id: UUID, requestId: UUID, ex: RedisIOException)     extends GetSetResponse
