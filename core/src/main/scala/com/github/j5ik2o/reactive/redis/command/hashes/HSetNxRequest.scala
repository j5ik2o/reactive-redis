package com.github.j5ik2o.reactive.redis.command.hashes

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final case class HSetNxRequest(id: UUID, key: String, field: String, value: String)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = HSetNxResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"HSETNX $key $field $value"

  override protected def responseParser: P[Expr] = P(integerReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (HSetNxSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (HSetNxSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (HSetNxFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait HSetNxResponse                                                    extends CommandResponse
final case class HSetNxSuspended(id: UUID, requestId: UUID)                    extends HSetNxResponse
final case class HSetNxSucceeded(id: UUID, requestId: UUID, isSet: Boolean)    extends HSetNxResponse
final case class HSetNxFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends HSetNxResponse
