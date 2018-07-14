package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import cats.Show
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import fastparse.byte.all._

case class SetRequest(id: UUID, key: String, value: String) extends CommandRequest with StringParsersSupport {
  override type Response = SetResponse

  override def asString: String = s"""SET $key "$value""""

  override protected def responseParser: P[Expr] = StringParsers.simpleStringReply

  override def parseResponse: Handler = {
    case SimpleExpr("OK") =>
      SetSucceeded(UUID.randomUUID(), id)
    case ErrorExpr(msg) =>
      SetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }

}

object SetRequest {

  def apply[A](id: UUID, key: String, value: A)(implicit s: Show[A]): SetRequest =
    new SetRequest(id, key, s.show(value))

}

sealed trait SetResponse                                              extends CommandResponse
case class SetSucceeded(id: UUID, requestId: UUID)                    extends SetResponse
case class SetFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SetResponse
