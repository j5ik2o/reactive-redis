package com.github.j5ik2o.reactive.redis.command.connection

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }

final class SelectRequest(val id: UUID, val index: Int) extends CommandRequest with StringParsersSupport {

  override type Response = SelectResponse
  override val isMasterOnly: Boolean = false

  override def asString: String = cs("SELECT", Some(index.toString))

  override protected def responseParser: P[Expr] = fastParse(simpleStringReply)

  override protected def parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (SelectSucceeded(UUID.randomUUID(), id), next)
    case (SimpleExpr(QUEUED), next) =>
      (SelectSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (SelectFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: SelectRequest =>
      id == that.id &&
      index == that.index
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, index)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"SelectRequest($id, $index)"

}

object SelectRequest {

  def apply(id: UUID, index: Int): SelectRequest = new SelectRequest(id, index)

  def unapply(self: SelectRequest): Option[(UUID, Int)] = Some((self.id, self.index))

  def create(id: UUID, index: Int): SelectRequest = apply(id, index)

}

sealed trait SelectResponse                                                    extends CommandResponse
final case class SelectSucceeded(id: UUID, requestId: UUID)                    extends SelectResponse
final case class SelectSuspended(id: UUID, requestId: UUID)                    extends SelectResponse
final case class SelectFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SelectResponse
