package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import cats.Show
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }

case class SetNxRequest(id: UUID, key: String, value: String) extends CommandRequest with StringParsersSupport {
  override type Response = SetNxResponse

  override def asString: String = s"""SETNX $key "$value""""

  override protected def responseParser: P[Expr] = StringParsers.integerReply

  override protected def parseResponse: Handler = {
    case NumberExpr(n) =>
      SetNxSucceeded(UUID.randomUUID(), id, n == 1)
    case ErrorExpr(msg) =>
      SetNxFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }

}

object SetNxRequest {

  def apply[A](id: UUID, key: String, value: A)(implicit s: Show[A]): SetNxRequest =
    new SetNxRequest(id, key, s.show(value))

}

sealed trait SetNxResponse                                              extends CommandResponse
case class SetNxSucceeded(id: UUID, requestId: UUID, isSet: Boolean)    extends SetNxResponse
case class SetNxFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SetNxResponse
