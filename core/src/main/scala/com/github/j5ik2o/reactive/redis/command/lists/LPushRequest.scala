package com.github.j5ik2o.reactive.redis.command.lists

import java.util.UUID

import cats.Show
import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

case class LPushRequest(id: UUID, key: String, values: NonEmptyList[String])
    extends CommandRequest
    with StringParsersSupport {

  override type Response = LPushResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"LPUSH $key ${values.toList.mkString(" ")}"

  override protected def responseParser: P[Expr] = P(integerReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (LPushSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (LPushSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (LPushFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

object LPushRequest {

  def apply[A](id: UUID, key: String, value: A)(implicit s: Show[A]): LPushRequest =
    apply(id, key, NonEmptyList.of(value))

  def apply[A](id: UUID, key: String, values: NonEmptyList[A])(implicit s: Show[A]): LPushRequest =
    new LPushRequest(id, key, values.map(s.show))

}

sealed trait LPushResponse                                              extends CommandResponse
case class LPushSuspended(id: UUID, requestId: UUID)                    extends LPushResponse
case class LPushSucceeded(id: UUID, requestId: UUID, value: Int)        extends LPushResponse
case class LPushFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends LPushResponse
