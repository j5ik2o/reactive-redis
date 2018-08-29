package com.github.j5ik2o.reactive.redis.command.hashes

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class HSetRequest(val id: UUID, val key: String, val field: String, val value: String)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = HSetResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("HSET", Some(key), Some(field), Some(value))

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (HSetSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (HSetSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (HSetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: HSetRequest =>
      id == that.id &&
      key == that.key &&
      field == that.field &&
      value == that.value
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key, field, value)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"HSetRequest($id, $key, $field, $value)"

}

object HSetRequest {

  def apply(id: UUID, key: String, field: String, value: String): HSetRequest = new HSetRequest(id, key, field, value)

  def unapply(self: HSetRequest): Option[(UUID, String, String, String)] =
    Some((self.id, self.key, self.field, self.value))

  def create(id: UUID, key: String, field: String, value: String): HSetRequest = apply(id, key, field, value)

}

sealed trait HSetResponse                                                    extends CommandResponse
final case class HSetSuspended(id: UUID, requestId: UUID)                    extends HSetResponse
final case class HSetSucceeded(id: UUID, requestId: UUID, isSet: Boolean)    extends HSetResponse
final case class HSetFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends HSetResponse
