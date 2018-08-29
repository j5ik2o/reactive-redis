package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class MoveRequest(val id: UUID, val key: String, val db: Int) extends CommandRequest with StringParsersSupport {

  override type Response = MoveResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("MOVE", Some(key), Some(db.toString))

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (MoveSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (MoveSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (MoveFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: MoveRequest =>
      id == that.id &&
      key == that.key &&
      db == that.db
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(id, key, db)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"MoveRequest($id, $key, $db)"

}

object MoveRequest {

  def apply(id: UUID, key: String, db: Int): MoveRequest = new MoveRequest(id, key, db)

  def unapply(self: MoveRequest): Option[(UUID, String, Int)] = Some((self.id, self.key, self.db))

  def create(id: UUID, key: String, db: Int): MoveRequest = apply(id, key, db)

}

sealed trait MoveResponse                                                    extends CommandResponse
final case class MoveSuspended(id: UUID, requestId: UUID)                    extends MoveResponse
final case class MoveSucceeded(id: UUID, requestId: UUID, isMoved: Boolean)  extends MoveResponse
final case class MoveFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends MoveResponse
