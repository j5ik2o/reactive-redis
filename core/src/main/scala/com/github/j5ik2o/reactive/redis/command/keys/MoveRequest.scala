package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final case class MoveRequest(id: UUID, key: String, db: Int) extends CommandRequest with StringParsersSupport {

  override type Response = MoveResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"MOVE $key $db"

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (MoveSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (MoveSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (MoveFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait MoveResponse                                                    extends CommandResponse
final case class MoveSuspended(id: UUID, requestId: UUID)                    extends MoveResponse
final case class MoveSucceeded(id: UUID, requestId: UUID, isMoved: Boolean)  extends MoveResponse
final case class MoveFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends MoveResponse
