package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

case class PersistRequest(id: UUID, key: String) extends CommandRequest with StringParsersSupport {

  override type Response = PersistResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"PERSIST $key"

  override protected def responseParser: P[Expr] = P(integerReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (PersistSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (PersistSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (PersistFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait PersistResponse                                               extends CommandResponse
case class PersistSuspended(id: UUID, requestId: UUID)                     extends PersistResponse
case class PersistSucceeded(id: UUID, requestId: UUID, isRemoved: Boolean) extends PersistResponse
case class PersistFailed(id: UUID, requestId: UUID, ex: RedisIOException)  extends PersistResponse