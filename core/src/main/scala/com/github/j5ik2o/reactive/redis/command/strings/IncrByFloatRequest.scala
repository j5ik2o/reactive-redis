package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr, StringOptExpr }
import fastparse.all._

@SuppressWarnings(Array("org.wartremover.warts.ToString"))
final class IncrByFloatRequest(val id: UUID, val key: String, val value: Double)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = IncrByFloatResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("INCRBYFLOAT", Some(key), Some(value.toString))

  override protected lazy val responseParser: P[Expr] = fastParse(bulkStringReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (StringOptExpr(s), next) =>
      (IncrByFloatSucceeded(UUID.randomUUID(), id, s.fold(0.0d)(_.toDouble)), next)
    case (SimpleExpr(QUEUED), next) =>
      (IncrByFloatSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (IncrByFloatFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: IncrByFloatRequest =>
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

  override def toString: String = s"IncrByFloatRequest($id, $key, $value)"
}

object IncrByFloatRequest {

  def apply(id: UUID, key: String, value: Double): IncrByFloatRequest = new IncrByFloatRequest(id, key, value)

  def unapply(self: IncrByFloatRequest): Option[(UUID, String, Double)] = Some((self.id, self.key, self.value))

  def create(id: UUID, key: String, value: Double): IncrByFloatRequest = apply(id, key, value)

}

sealed trait IncrByFloatResponse                                                    extends CommandResponse
final case class IncrByFloatSuspended(id: UUID, requestId: UUID)                    extends IncrByFloatResponse
final case class IncrByFloatSucceeded(id: UUID, requestId: UUID, value: Double)     extends IncrByFloatResponse
final case class IncrByFloatFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends IncrByFloatResponse
