package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model._
import fastparse.all._

final case class RandomKeyRequest(id: UUID) extends CommandRequest with StringParsersSupport {

  override type Response = RandomKeyResponse
  override val isMasterOnly: Boolean = true

  override def asString: String = cs("RANDOMKEY")

  override protected def responseParser: P[Expr] = fastParse(bulkStringReply | simpleStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (StringOptExpr(s), next) =>
      (RandomKeySucceeded(UUID.randomUUID(), id, s), next)
    case (SimpleExpr(QUEUED), next) =>
      (RandomKeySuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (RandomKeyFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait RandomKeyResponse                                                        extends CommandResponse
final case class RandomKeySucceeded(id: UUID, requestId: UUID, value: Option[String]) extends RandomKeyResponse
final case class RandomKeySuspended(id: UUID, requestId: UUID)                        extends RandomKeyResponse
final case class RandomKeyFailed(id: UUID, requestId: UUID, ex: RedisIOException)     extends RandomKeyResponse
