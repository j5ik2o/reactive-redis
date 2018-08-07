package com.github.j5ik2o.reactive.redis.command.lists

import java.util.UUID

import cats.Show
import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class RPushRequest(val id: UUID, val key: String, val values: NonEmptyList[String])
    extends CommandRequest
    with StringParsersSupport {

  override type Response = RPushResponse
  override val isMasterOnly: Boolean = true

  override def asString: String = s"RPUSH $key ${values.toList.mkString(" ")}"

  override protected def responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (RPushSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (RPushSuspend(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (RPushFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

object RPushRequest {
  def apply[A](id: UUID, key: String, value: A)(implicit s: Show[A]): RPushRequest =
    new RPushRequest(id, key, NonEmptyList.one(s.show(value)))
  def apply[A](id: UUID, key: String, values: NonEmptyList[A])(implicit s: Show[A]): RPushRequest =
    new RPushRequest(id, key, values.map(v => s.show(v)))
}

sealed trait RPushResponse                                                    extends CommandResponse
final case class RPushSucceeded(id: UUID, requestId: UUID, value: Long)       extends RPushResponse
final case class RPushSuspend(id: UUID, requestId: UUID)                      extends RPushResponse
final case class RPushFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends RPushResponse
