package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }

case class MSetRequest(id: UUID, values: Map[String, Any]) extends CommandRequest with StringParsersSupport {
  override type Response = MSetResponse

  override def asString: String = {
    val keyWithValues = values.foldLeft("") {
      case (r, (k, v)) =>
        r + s""" $k "$v""""
    }
    s"MSET $keyWithValues"
  }

  override protected def responseParser: P[Expr] = StringParsers.simpleStringReply

  override protected def parseResponse: Handler = {
    case SimpleExpr("OK") =>
      MSetSucceeded(UUID.randomUUID(), id)
    case ErrorExpr(msg) =>
      MSetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }

}

sealed trait MSetResponse                                              extends CommandResponse
case class MSetSucceeded(id: UUID, requestId: UUID)                    extends MSetResponse
case class MSetFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends MSetResponse