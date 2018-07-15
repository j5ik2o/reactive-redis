package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import cats.Show
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandResponse, SimpleCommandRequest, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }

import scala.concurrent.duration.FiniteDuration

case class SetExRequest(id: UUID, key: String, expires: FiniteDuration, value: String)
    extends SimpleCommandRequest
    with StringParsersSupport {
  override type Response = SetExResponse

  override def asString: String = s"""SETEX $key ${expires.toSeconds} "$value""""

  override protected def responseParser: P[Expr] = StringParsers.simpleStringReply

  override protected def parseResponse: Handler = {
    case (SimpleExpr("OK"), next) =>
      (SetExSucceeded(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (SetExFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

object SetExRequest {

  def apply[A](id: UUID, key: String, expires: FiniteDuration, value: A)(implicit s: Show[A]): SetExRequest =
    new SetExRequest(id, key, expires, s.show(value))

}

sealed trait SetExResponse                                              extends CommandResponse
case class SetExSucceeded(id: UUID, requestId: UUID)                    extends SetExResponse
case class SetExFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SetExResponse
