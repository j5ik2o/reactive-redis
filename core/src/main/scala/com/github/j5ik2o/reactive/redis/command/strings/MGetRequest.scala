package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ArrayExpr, ErrorExpr, Expr, StringOptExpr }

case class MGetRequest(id: UUID, keys: Seq[String]) extends CommandRequest with StringParsersSupport {
  override type Response = MGetResponse

  override def asString: String = s"MGET ${keys.mkString(" ")}"

  override protected def responseParser: P[Expr] = StringParsers.stringOptArrayReply

  override protected def parseResponse: Handler = {
    case ArrayExpr(values) =>
      val _values = values.asInstanceOf[Seq[StringOptExpr]]
      MGetSucceeded(UUID.randomUUID(), id, _values.map(_.vOp))
    case ErrorExpr(msg) =>
      MGetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))

  }
}

sealed trait MGetResponse                                                        extends CommandResponse
case class MGetSucceeded(id: UUID, requestId: UUID, values: Seq[Option[String]]) extends MGetResponse
case class MGetFailed(id: UUID, requestId: UUID, ex: Exception)                  extends MGetResponse
