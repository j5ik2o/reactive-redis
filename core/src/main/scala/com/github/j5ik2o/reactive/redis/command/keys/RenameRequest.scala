package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import fastparse.all._

final class RenameRequest(val id: UUID, val key: String, val newKey: String)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = RenameResponse
  override val isMasterOnly: Boolean = true

  override def asString: String = cs("RENAME", Some(key), Some(newKey))

  override protected def responseParser: P[Expr] = fastParse(simpleStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (RenameSucceeded(UUID.randomUUID(), id), next)
    case (SimpleExpr(QUEUED), next) =>
      (RenameSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (RenameFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: RenameRequest =>
      id == that.id &&
      key == that.key &&
      newKey == that.newKey
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key, newKey)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"RenameRequest($id, $key, $newKey)"
}

object RenameRequest {

  def apply(id: UUID, key: String, newKey: String): RenameRequest = new RenameRequest(id, key, newKey)

  def unapply(self: RenameRequest): Option[(UUID, String, String)] = Some((self.id, self.key, self.newKey))

  def create(id: UUID, key: String, newKey: String): RenameRequest = new RenameRequest(id, key, newKey)
}

sealed trait RenameResponse                                                    extends CommandResponse
final case class RenameSucceeded(id: UUID, requestId: UUID)                    extends RenameResponse
final case class RenameSuspended(id: UUID, requestId: UUID)                    extends RenameResponse
final case class RenameFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends RenameResponse
