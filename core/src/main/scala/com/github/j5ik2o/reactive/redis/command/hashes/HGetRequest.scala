package com.github.j5ik2o.reactive.redis.command.hashes

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr, StringOptExpr }
import fastparse.all._

final case class HGetRequest(id: UUID, key: String, field: String) extends CommandRequest with StringParsersSupport {

  override type Response = HGetResponse

  override val isMasterOnly: Boolean = false

  override def asString: String = s"HGET $key $field"

  override protected def responseParser: P[Expr] = wrap(bulkStringReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (StringOptExpr(s), next) =>
      (HGetSucceeded(UUID.randomUUID(), id, s), next)
    case (SimpleExpr(QUEUED), next) =>
      (HGetSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (HGetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait HGetResponse                                                        extends CommandResponse
final case class HGetSuspended(id: UUID, requestId: UUID)                        extends HGetResponse
final case class HGetSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends HGetResponse
final case class HGetFailed(id: UUID, requestId: UUID, ex: RedisIOException)     extends HGetResponse
