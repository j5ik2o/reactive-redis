package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class AppendRequest(val id: UUID, val key: String, val value: String)
    extends CommandRequest
    with StringParsersSupport {
  override val isMasterOnly: Boolean = true

  override type Response = AppendResponse

  override def asString: String = cs("APPEND", Some(key), Some(value))

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (AppendSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (AppendSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (AppendFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: AppendRequest =>
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

  override def toString: String = s"AppendRequest($id, $key, $value)"

}

object AppendRequest {

  def apply(id: UUID, key: String, value: String): AppendRequest = new AppendRequest(id, key, value)

  def unapply(self: AppendRequest): Option[(UUID, String, String)] = Some((self.id, self.key, self.value))

  def create(id: UUID, key: String, value: String): AppendRequest = apply(id, key, value)

}

sealed trait AppendResponse                                                    extends CommandResponse
final case class AppendSuspended(id: UUID, requestId: UUID)                    extends AppendResponse
final case class AppendSucceeded(id: UUID, requestId: UUID, value: Long)       extends AppendResponse
final case class AppendFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends AppendResponse
