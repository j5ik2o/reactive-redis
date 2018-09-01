package com.github.j5ik2o.reactive.redis.command.lists

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model._
import fastparse.all._

import scala.concurrent.duration.Duration

final class BRPopLPushRequest(val id: UUID, val source: String, val destination: String, val timeout: Duration)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = BRPopLPushResponse
  override val isMasterOnly: Boolean = true

  override def asString: String = cs("BRPOPLPUSH", Some(source), Some(destination), Some(timeout.toSeconds.toString))

  override protected def responseParser: P[Expr] = fastParse(bulkStringReply | simpleStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (StringOptExpr(v), next) =>
      (BRPopLPushSucceeded(UUID.randomUUID(), id, v), next)
    case (SimpleExpr(QUEUED), next) =>
      (BRPopLPushSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (BRPopLPushFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: BRPopLPushRequest =>
      id == that.id &&
      source == that.source &&
      destination == that.destination &&
      timeout == that.timeout
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, source, destination, timeout)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"BRPopLPushRequest($id, $source, $destination, $timeout)"
}

object BRPopLPushRequest {

  def apply(id: UUID, source: String, destination: String, timeout: Duration): BRPopLPushRequest =
    new BRPopLPushRequest(id, source, destination, timeout)

  def unapply(self: BRPopLPushRequest): Option[(UUID, String, String, Duration)] =
    Some((self.id, self.source, self.destination, self.timeout))

  def create(id: UUID, source: String, destination: String, timeout: Duration): BRPopLPushRequest =
    apply(id, source, destination, timeout)

}

sealed trait BRPopLPushResponse                                                        extends CommandResponse
final case class BRPopLPushSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends BRPopLPushResponse
final case class BRPopLPushSuspended(id: UUID, requestId: UUID)                        extends BRPopLPushResponse
final case class BRPopLPushFailed(id: UUID, requestId: UUID, ex: RedisIOException)     extends BRPopLPushResponse
