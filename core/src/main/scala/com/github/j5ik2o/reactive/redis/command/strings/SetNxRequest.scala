package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import cats.Show
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final case class SetNxRequest(id: UUID, key: String, value: String) extends CommandRequest with StringParsersSupport {

  override type Response = SetNxResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"""SETNX $key "$value""""

  override protected def responseParser: P[Expr] = wrap(integerReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (SetNxSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (SetNxSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (SetNxFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

object SetNxRequest {

  def apply[A](id: UUID, key: String, value: A)(implicit s: Show[A]): SetNxRequest =
    new SetNxRequest(id, key, s.show(value))

}

sealed trait SetNxResponse                                                    extends CommandResponse
final case class SetNxSuspended(id: UUID, requestId: UUID)                    extends SetNxResponse
final case class SetNxSucceeded(id: UUID, requestId: UUID, isSet: Boolean)    extends SetNxResponse
final case class SetNxFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SetNxResponse
