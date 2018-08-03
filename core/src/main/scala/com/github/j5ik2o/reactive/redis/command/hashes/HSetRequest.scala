package com.github.j5ik2o.reactive.redis.command.hashes

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final case class HSetRequest(id: UUID, key: String, field: String, value: String)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = HSetResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"""HSET $key $field "$value""""

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (HSetSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (HSetSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (HSetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait HSetResponse                                                    extends CommandResponse
final case class HSetSuspended(id: UUID, requestId: UUID)                    extends HSetResponse
final case class HSetSucceeded(id: UUID, requestId: UUID, isSet: Boolean)    extends HSetResponse
final case class HSetFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends HSetResponse
