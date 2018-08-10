package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final case class DecrByRequest(id: UUID, key: String, value: Int) extends CommandRequest with StringParsersSupport {

  override type Response = DecrByResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("DECRBY", Some(key), Some(value.toString))

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (DecrBySucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (DecrBySuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (DecrByFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait DecrByResponse                                                    extends CommandResponse
final case class DecrBySuspended(id: UUID, requestId: UUID)                    extends DecrByResponse
final case class DecrBySucceeded(id: UUID, requestId: UUID, value: Long)       extends DecrByResponse
final case class DecrByFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends DecrByResponse
