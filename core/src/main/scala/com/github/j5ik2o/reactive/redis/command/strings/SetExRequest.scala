package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import cats.Show
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import fastparse.all._

import scala.concurrent.duration.FiniteDuration

final case class SetExRequest(id: UUID, key: String, expires: FiniteDuration, value: String)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = SetExResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"""SETEX $key ${expires.toSeconds} $value"""

  override protected def responseParser: P[Expr] = P(simpleStringReply)

  override protected def parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (SetExSucceeded(UUID.randomUUID(), id), next)
    case (SimpleExpr(QUEUED), next) =>
      (SetExSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (SetExFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

object SetExRequest {

  def apply[A](id: UUID, key: String, expires: FiniteDuration, value: A)(implicit s: Show[A]): SetExRequest =
    new SetExRequest(id, key, expires, s.show(value))

}

sealed trait SetExResponse                                                    extends CommandResponse
final case class SetExSuspended(id: UUID, requestId: UUID)                    extends SetExResponse
final case class SetExSucceeded(id: UUID, requestId: UUID)                    extends SetExResponse
final case class SetExFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SetExResponse
