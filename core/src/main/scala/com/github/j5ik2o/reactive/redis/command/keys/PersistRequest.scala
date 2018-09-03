package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class PersistRequest(val id: UUID, val key: String) extends CommandRequest with StringParsersSupport {

  override type Response = PersistResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("PERSIST", Some(key))

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (PersistSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (PersistSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (PersistFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: PersistRequest =>
      id == that.id &&
      key == that.key
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"PersistRequest($id, $key)"

}

object PersistRequest {

  def apply(id: UUID, key: String): PersistRequest = new PersistRequest(id, key)

  def create(id: UUID, key: String): PersistRequest = apply(id, key)
}

sealed trait PersistResponse                                                     extends CommandResponse
final case class PersistSuspended(id: UUID, requestId: UUID)                     extends PersistResponse
final case class PersistSucceeded(id: UUID, requestId: UUID, isRemoved: Boolean) extends PersistResponse
final case class PersistFailed(id: UUID, requestId: UUID, ex: RedisIOException)  extends PersistResponse
