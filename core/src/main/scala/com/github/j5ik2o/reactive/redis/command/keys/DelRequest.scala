package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class DelRequest(val id: UUID, val keys: NonEmptyList[String]) extends CommandRequest with StringParsersSupport {

  // val key: String = keys.head

  override type Response = DelResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("DEL", keys.map(Some(_)).toList: _*)

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (DelSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (DelSuspended(id, id), next)
    case (ErrorExpr(msg), next) =>
      (DelFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

object DelRequest {

  def apply(id: UUID, key: String, keys: String*): DelRequest = new DelRequest(id, NonEmptyList.of(key, keys: _*))

  def apply(id: UUID, keys: NonEmptyList[String]): DelRequest = new DelRequest(id, keys)

}

sealed trait DelResponse                                                    extends CommandResponse
final case class DelSuspended(id: UUID, requestId: UUID)                    extends DelResponse
final case class DelSucceeded(id: UUID, requestId: UUID, value: Long)       extends DelResponse
final case class DelFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends DelResponse
