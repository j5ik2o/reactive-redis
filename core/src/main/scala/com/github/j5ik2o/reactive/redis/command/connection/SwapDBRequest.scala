package com.github.j5ik2o.reactive.redis.command.connection

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import fastparse.all._

final class SwapDBRequest(val id: UUID, val index0: Int, val index1: Int)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = SwapDBResponse
  override val isMasterOnly: Boolean = true

  override def asString: String = cs("SWAPDB", Some(index0.toString), Some(index1.toString))

  override protected def responseParser: P[Expr] = fastParse(simpleStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (SwapDBSucceeded(UUID.randomUUID(), id), next)
    case (SimpleExpr(QUEUED), next) =>
      (SwapDBSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (SwapDBFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: SwapDBRequest =>
      id == that.id &&
      index0 == that.index0 &&
      index1 == that.index1
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(id, index0, index1)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"SwapDBRequest($id, $index0, $index1)"

}

object SwapDBRequest {

  def apply(id: UUID, index0: Int, index1: Int): SwapDBRequest = new SwapDBRequest(id, index0, index1)

  def unapply(self: SwapDBRequest): Option[(UUID, Int, Int)] = Some((self.id, self.index0, self.index1))

  def create(id: UUID, index0: Int, index1: Int): SwapDBRequest = apply(id, index0, index1)

}

sealed trait SwapDBResponse                                                    extends CommandResponse
final case class SwapDBSucceeded(id: UUID, requestId: UUID)                    extends SwapDBResponse
final case class SwapDBSuspended(id: UUID, requestId: UUID)                    extends SwapDBResponse
final case class SwapDBFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SwapDBResponse
