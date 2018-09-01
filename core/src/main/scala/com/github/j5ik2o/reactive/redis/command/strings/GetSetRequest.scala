package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr, StringOptExpr }
import fastparse.all._

final class GetSetRequest(val id: UUID, val key: String, val value: String)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = GetSetResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("GETSET", Some(key), Some(value))

  override protected lazy val responseParser: P[Expr] = fastParse(bulkStringReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (StringOptExpr(s), next) =>
      (GetSetSucceeded(UUID.randomUUID(), id, s), next)
    case (SimpleExpr(QUEUED), next) =>
      (GetSetSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (GetSetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: GetSetRequest =>
      id == that.id &&
      key == that.key &&
      value == that.value
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key, value)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"GetSetRequest($id, $key, $value)"

}

object GetSetRequest {

  def apply(id: UUID, key: String, value: String): GetSetRequest = new GetSetRequest(id, key, value)

  def unapply(self: GetSetRequest): Option[(UUID, String, String)] = Some((self.id, self.key, self.value))

  def create(id: UUID, key: String, value: String): GetSetRequest = apply(id, key, value)

}

sealed trait GetSetResponse                                                        extends CommandResponse
final case class GetSetSuspended(id: UUID, requestId: UUID)                        extends GetSetResponse
final case class GetSetSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends GetSetResponse
final case class GetSetFailed(id: UUID, requestId: UUID, ex: RedisIOException)     extends GetSetResponse
