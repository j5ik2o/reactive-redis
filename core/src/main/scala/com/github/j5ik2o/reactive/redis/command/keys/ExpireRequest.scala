package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import scala.concurrent.duration._
import fastparse.all._

import scala.concurrent.duration.FiniteDuration

final class ExpireRequest(val id: UUID, val key: String, val seconds: FiniteDuration)
    extends CommandRequest
    with StringParsersSupport {

  require(seconds.gteq(1 seconds))

  override type Response = ExpireResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("EXPIRE", Some(key), Some(seconds.toSeconds.toString))

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (ExpireSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (ExpireSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (ExpireFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: ExpireRequest =>
      id == that.id &&
      key == that.key &&
      seconds == that.seconds
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key, seconds)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"ExpireRequest($id, $key, $seconds)"

}

object ExpireRequest {

  def apply(id: UUID, key: String, seconds: FiniteDuration): ExpireRequest = new ExpireRequest(id, key, seconds)

  def unapply(self: ExpireRequest): Option[(UUID, String, FiniteDuration)] = Some((self.id, self.key, self.seconds))

  def create(id: UUID, key: String, seconds: FiniteDuration): ExpireRequest = apply(id, key, seconds)

}

sealed trait ExpireResponse                                                    extends CommandResponse
final case class ExpireSuspended(id: UUID, requestId: UUID)                    extends ExpireResponse
final case class ExpireSucceeded(id: UUID, requestId: UUID, isSet: Boolean)    extends ExpireResponse
final case class ExpireFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends ExpireResponse
