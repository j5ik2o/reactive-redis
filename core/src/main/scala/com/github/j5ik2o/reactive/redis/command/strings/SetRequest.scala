package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import cats.Show
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandResponse, SimpleCommandRequest, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }

case class SetRequest(id: UUID, key: String, value: String) extends SimpleCommandRequest with StringParsersSupport {
  override type Response = SetResponse

  override def asString: String = s"""SET $key "$value""""

  override protected def responseParser: P[Expr] = simpleStringReply

  override def parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (SetSucceeded(UUID.randomUUID(), id), next)
    case (SimpleExpr(QUEUED), next) =>
      (SetSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (SetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

object SetRequest {

  def apply[A](id: UUID, key: String, value: A)(implicit s: Show[A]): SetRequest =
    new SetRequest(id, key, s.show(value))

}

sealed trait SetResponse                                              extends CommandResponse
case class SetSuspended(id: UUID, requestId: UUID)                    extends SetResponse
case class SetSucceeded(id: UUID, requestId: UUID)                    extends SetResponse
case class SetFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SetResponse
