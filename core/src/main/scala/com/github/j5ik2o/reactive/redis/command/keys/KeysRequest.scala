package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ArrayExpr, ErrorExpr, Expr, StringOptExpr }

case class KeysRequest(id: UUID, pattern: String) extends CommandRequest with StringParsersSupport {
  override type Response = KeysResponse

  override def asString: String = s"KEYS $pattern"

  override protected def responseParser: P[Expr] = StringParsers.stringOptArrayReply

  override protected def parseResponse: Handler = {
    case ArrayExpr(values) =>
      val _values = values.asInstanceOf[Seq[StringOptExpr]]
      KeysSucceeded(UUID.randomUUID(), id, _values.map(_.vOp))
    case ErrorExpr(msg) =>
      KeysFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }
}

sealed trait KeysResponse                                                        extends CommandResponse
case class KeysSucceeded(id: UUID, requestId: UUID, values: Seq[Option[String]]) extends KeysResponse
case class KeysFailed(id: UUID, requestId: UUID, ex: RedisIOException)           extends KeysResponse
