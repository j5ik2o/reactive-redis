package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import cats.Show
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }

case class SetRangeRequest(id: UUID, key: String, range: Int, value: String)
    extends CommandRequest
    with StringParsersSupport {
  override type Response = SetRangeResponse

  override def asString: String = s"""SETRANGE $key $range "$value""""

  override protected def responseParser: P[Expr] = StringParsers.integerReply

  override protected def parseResponse: Handler = {
    case NumberExpr(n) =>
      SetRangeSucceeded(UUID.randomUUID(), id, n)
    case ErrorExpr(msg) =>
      SetRangeFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }

}

object SetRangeRequest {

  def apply[A](id: UUID, key: String, range: Int, value: A)(implicit s: Show[A]): SetRangeRequest =
    new SetRangeRequest(id, key, range, s.show(value))

}

sealed trait SetRangeResponse                                              extends CommandResponse
case class SetRangeSucceeded(id: UUID, requestId: UUID, value: Int)        extends SetRangeResponse
case class SetRangeFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SetRangeResponse