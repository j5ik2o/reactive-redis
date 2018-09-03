package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }

import scala.concurrent.duration.Duration
import fastparse.all._

final class WaitReplicasRequest(val id: UUID, val numOfReplicas: Int, val timeout: Duration)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = WaitReplicasResponse
  override val isMasterOnly: Boolean = true

  override def asString: String = cs("WAIT", Some(numOfReplicas.toString), Some(timeout.toSeconds.toString))

  override protected def responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (WaitReplicasSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (WaitReplicasSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (WaitReplicasFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: WaitReplicasRequest =>
      id == that.id &&
      numOfReplicas == that.numOfReplicas &&
      timeout == that.timeout
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(id, numOfReplicas, timeout)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"WaitReplicasRequest($id, $numOfReplicas, $timeout)"

}

object WaitReplicasRequest {

  def apply(id: UUID, numOfReplicas: Int, timeout: Duration): WaitReplicasRequest =
    new WaitReplicasRequest(id, numOfReplicas, timeout)

  def unapply(self: WaitReplicasRequest): Option[(UUID, Int, Duration)] =
    Some((self.id, self.numOfReplicas, self.timeout))

  def create(id: UUID, numOfReplicas: Int, timeout: Duration): WaitReplicasRequest = apply(id, numOfReplicas, timeout)

}

sealed trait WaitReplicasResponse extends CommandResponse

final case class WaitReplicasSucceeded(id: UUID, requestId: UUID, value: Long)       extends WaitReplicasResponse
final case class WaitReplicasSuspended(id: UUID, requestId: UUID)                    extends WaitReplicasResponse
final case class WaitReplicasFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends WaitReplicasResponse
