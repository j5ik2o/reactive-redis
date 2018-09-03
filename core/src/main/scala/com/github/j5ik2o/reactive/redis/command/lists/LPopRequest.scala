package com.github.j5ik2o.reactive.redis.command.lists

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr, StringOptExpr }
import fastparse.all._

final class LPopRequest(val id: UUID, val key: String) extends CommandRequest with StringParsersSupport {

  override type Response = LPopResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("LPOP", Some(key))

  override protected lazy val responseParser: P[Expr] = fastParse(bulkStringReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (StringOptExpr(s), next) =>
      (LPopSucceeded(UUID.randomUUID(), id, s), next)
    case (SimpleExpr(QUEUED), next) =>
      (LPopSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (LPopFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: LPopRequest =>
      id == that.id &&
      key == that.key
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"LPopRequest($id, $key)"

}

object LPopRequest {

  def apply(id: UUID, key: String): LPopRequest = new LPopRequest(id, key)

  def unapply(self: LPopRequest): Option[(UUID, String)] = Some((self.id, self.key))

  def create(id: UUID, key: String): LPopRequest = apply(id, key)

}

sealed trait LPopResponse                                                        extends CommandResponse
final case class LPopSuspended(id: UUID, requestId: UUID)                        extends LPopResponse
final case class LPopSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends LPopResponse
final case class LPopFailed(id: UUID, requestId: UUID, ex: RedisIOException)     extends LPopResponse
