package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }

case class MSetNxRequest(id: UUID, values: Map[String, Any]) extends CommandRequest with StringParsersSupport {
  override type Response = MSetNxResponse

  override def asString: String = {
    val keyWithValues = values.foldLeft("") {
      case (r, (k, v)) =>
        r + s""" $k "$v""""
    }
    s"MSETNX $keyWithValues"
  }

  override protected def responseParser: P[Expr] = StringParsers.integerReply

  override protected def parseResponse: Handler = {
    case NumberExpr(n) =>
      MSetNxSucceeded(UUID.randomUUID(), id, n == 1)
    case ErrorExpr(msg) =>
      MSetNxFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }

}

sealed trait MSetNxResponse                                              extends CommandResponse
case class MSetNxSucceeded(id: UUID, requestId: UUID, isSet: Boolean)    extends MSetNxResponse
case class MSetNxFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends MSetNxResponse
