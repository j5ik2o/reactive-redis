package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }

case class MoveRequest(id: UUID, key: String, db: Int) extends CommandRequest with StringParsersSupport {
  override type Response = MoveResponse

  override def asString: String = s"MOVE $key $db"

  override protected def responseParser: P[Expr] = StringParsers.integerReply

  override protected def parseResponse: Handler = {
    case NumberExpr(n) =>
      MoveSucceeded(UUID.randomUUID(), id, n == 1)
    case ErrorExpr(msg) =>
      MoveFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }

}

sealed trait MoveResponse                                              extends CommandResponse
case class MoveSucceeded(id: UUID, requestId: UUID, isMoved: Boolean)  extends MoveResponse
case class MoveFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends MoveResponse
