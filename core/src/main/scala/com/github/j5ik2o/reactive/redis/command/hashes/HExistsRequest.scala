package com.github.j5ik2o.reactive.redis.command.hashes

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

case class HExistsRequest(id: UUID, key: String, field: String) extends CommandRequest with StringParsersSupport {

  override type Response = HExistsResponse
  override val isMasterOnly: Boolean = false

  override def asString: String = s"HEXISTS $key $field"

  override protected def responseParser: P[Expr] = P(integerReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (HExistsSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (HExistsSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (HExistsFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait HExistsResponse                                              extends CommandResponse
case class HExistsSuspended(id: UUID, requestId: UUID)                    extends HExistsResponse
case class HExistsSucceeded(id: UUID, requestId: UUID, isExists: Boolean) extends HExistsResponse
case class HExistsFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends HExistsResponse