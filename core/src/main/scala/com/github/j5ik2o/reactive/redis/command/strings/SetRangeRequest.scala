package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import cats.Show
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final case class SetRangeRequest(id: UUID, key: String, range: Int, value: String)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = SetRangeResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"""SETRANGE $key $range "$value""""

  override protected def responseParser: P[Expr] = P(integerReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (SetRangeSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (SetRangeSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (SetRangeFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

object SetRangeRequest {

  def apply[A](id: UUID, key: String, range: Int, value: A)(implicit s: Show[A]): SetRangeRequest =
    new SetRangeRequest(id, key, range, s.show(value))

}

sealed trait SetRangeResponse                                                    extends CommandResponse
final case class SetRangeSuspended(id: UUID, requestId: UUID)                    extends SetRangeResponse
final case class SetRangeSucceeded(id: UUID, requestId: UUID, value: Int)        extends SetRangeResponse
final case class SetRangeFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SetRangeResponse
