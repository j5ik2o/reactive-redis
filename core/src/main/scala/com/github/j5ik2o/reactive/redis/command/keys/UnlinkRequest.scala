package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class UnlinkRequest(val id: UUID, val keys: NonEmptyList[String])
    extends CommandRequest
    with StringParsersSupport {

  override type Response = UnlinkResponse
  override val isMasterOnly: Boolean = true

  override def asString: String = cs("UNLINK", keys.toList.map(Some(_)): _*)

  override protected def responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (UnlinkSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (UnlinkSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (UnlinkFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: UnlinkRequest =>
      id == that.id &&
      keys == that.keys
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, keys)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"UnlinkRequest($id, $keys)"

}

object UnlinkRequest {

  def apply(id: UUID, key: String, keys: String*): UnlinkRequest = apply(id, NonEmptyList.of(key, keys: _*))

  def apply(id: UUID, keys: NonEmptyList[String]): UnlinkRequest = new UnlinkRequest(id, keys)

  def unapply(self: UnlinkRequest): Option[(UUID, NonEmptyList[String])] = Some((self.id, self.keys))

  def create(id: UUID, key: String, keys: String*): UnlinkRequest = apply(id, key, keys: _*)

  def create(id: UUID, keys: NonEmptyList[String]): UnlinkRequest = apply(id, keys)

}

sealed trait UnlinkResponse                                                    extends CommandResponse
final case class UnlinkSucceeded(id: UUID, requestId: UUID, value: Long)       extends UnlinkResponse
final case class UnlinkSuspended(id: UUID, requestId: UUID)                    extends UnlinkResponse
final case class UnlinkFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends UnlinkResponse
