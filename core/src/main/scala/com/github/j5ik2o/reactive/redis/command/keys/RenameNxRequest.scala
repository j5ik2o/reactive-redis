package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class RenameNxRequest(val id: UUID, val key: String, val newKey: String)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = RenameNxResponse
  override val isMasterOnly: Boolean = true

  override def asString: String = cs("RENAMENX", Some(key), Some(newKey))

  override protected def responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (RenameNxSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (RenameNxSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (RenameNxFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: RenameNxRequest =>
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

  override def toString: String = s"RenameNxRequest($id, $key, $newKey)"

}

object RenameNxRequest {

  def apply(id: UUID, key: String, newKey: String): RenameNxRequest = new RenameNxRequest(id, key, newKey)

  def unapply(self: RenameNxRequest): Option[(UUID, String, String)] = Some((self.id, self.key, self.newKey))

  def create(id: UUID, key: String, newKey: String): RenameNxRequest = apply(id, key, newKey)

}

sealed trait RenameNxResponse                                                     extends CommandResponse
final case class RenameNxSucceeded(id: UUID, requestId: UUID, isRenamed: Boolean) extends RenameNxResponse
final case class RenameNxSuspended(id: UUID, requestId: UUID)                     extends RenameNxResponse
final case class RenameNxFailed(id: UUID, requestId: UUID, ex: RedisIOException)  extends RenameNxResponse
