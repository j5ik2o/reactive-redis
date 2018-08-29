package com.github.j5ik2o.reactive.redis.command.hashes

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr, StringOptExpr }
import fastparse.all._

final class HGetRequest(val id: UUID, val key: String, val field: String)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = HGetResponse

  override val isMasterOnly: Boolean = false

  override def asString: String = cs("HGET", Some(key), Some(field))

  override protected lazy val responseParser: P[Expr] = fastParse(bulkStringReply | simpleStringReply)

  override protected lazy val parseResponse: Handler = {
    case (StringOptExpr(s), next) =>
      (HGetSucceeded(UUID.randomUUID(), id, s), next)
    case (SimpleExpr(QUEUED), next) =>
      (HGetSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (HGetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: HGetRequest =>
      id == that.id &&
      key == that.key &&
      field == that.field
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key, field)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"HGetRequest($id, $key, $field)"

}

object HGetRequest {

  def apply(id: UUID, key: String, field: String): HGetRequest = new HGetRequest(id, key, field)

  def unapply(self: HGetRequest): Option[(UUID, String, String)] = Some((self.id, self.key, self.field))

  def create(id: UUID, key: String, field: String): HGetRequest = apply(id, key, field)
}

sealed trait HGetResponse                                                        extends CommandResponse
final case class HGetSuspended(id: UUID, requestId: UUID)                        extends HGetResponse
final case class HGetSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends HGetResponse
final case class HGetFailed(id: UUID, requestId: UUID, ex: RedisIOException)     extends HGetResponse
