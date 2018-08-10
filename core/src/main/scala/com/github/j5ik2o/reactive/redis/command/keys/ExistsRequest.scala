package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class ExistsRequest(val id: UUID, val keys: NonEmptyList[String])
    extends CommandRequest
    with StringParsersSupport {

  override def toString: String = s"ExistsRequest($id, $keys)"

  override type Response = ExistsResponse

  override val isMasterOnly: Boolean = false

  override def asString: String = s"EXISTS ${keys.toList.mkString(" ")}"

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (ExistsSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (ExistsSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (ExistsFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

object ExistsRequest {
  def apply(id: UUID, keys: NonEmptyList[String]): ExistsRequest = new ExistsRequest(id, keys)
  def apply(id: UUID, key: String, keys: String*): ExistsRequest = apply(id, NonEmptyList.of(key, keys: _*))
}

sealed trait ExistsResponse                                                    extends CommandResponse
final case class ExistsSuspended(id: UUID, requestId: UUID)                    extends ExistsResponse
final case class ExistsSucceeded(id: UUID, requestId: UUID, isExists: Boolean) extends ExistsResponse
final case class ExistsFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends ExistsResponse
