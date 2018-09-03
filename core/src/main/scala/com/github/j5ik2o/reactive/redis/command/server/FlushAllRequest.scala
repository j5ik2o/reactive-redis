package com.github.j5ik2o.reactive.redis.command.server

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import fastparse.all._

final class FlushAllRequest(val id: UUID, val async: Boolean) extends CommandRequest with StringParsersSupport {

  override type Response = FlushAllResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("FLUSHALL", if (async) Some("ASYNC") else None)

  override protected lazy val responseParser: P[Expr] = fastParse(simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (FlushAllSucceeded(UUID.randomUUID(), id), next)
    case (SimpleExpr(QUEUED), next) =>
      (FlushAllSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (FlushAllFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: FlushAllRequest =>
      id == that.id &&
      async == that.async
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(id, async)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"FlushAllRequest($id, $async)"

}

object FlushAllRequest {

  def apply(id: UUID, async: Boolean = false): FlushAllRequest = new FlushAllRequest(id, async)

  def unapply(self: FlushAllRequest): Option[(UUID, Boolean)] = Some((self.id, self.async))

  def create(id: UUID, async: Boolean = false): FlushAllRequest = apply(id, async)

}

sealed trait FlushAllResponse                                                    extends CommandResponse
final case class FlushAllSuspended(id: UUID, requestId: UUID)                    extends FlushAllResponse
final case class FlushAllSucceeded(id: UUID, requestId: UUID)                    extends FlushAllResponse
final case class FlushAllFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends FlushAllResponse
