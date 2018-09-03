package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import fastparse.all._

@SuppressWarnings(Array("org.wartremover.warts.ToString"))
final class MSetRequest(val id: UUID, val values: Map[String, Any]) extends CommandRequest with StringParsersSupport {

  override type Response = MSetResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = {
    val params = values.toSeq.flatMap { case (k, v) => Seq(k, v.toString) }.map(Some(_))
    cs("MSET", params: _*)
  }

  override protected lazy val responseParser: P[Expr] = fastParse(simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (MSetSucceeded(UUID.randomUUID(), id), next)
    case (SimpleExpr(QUEUED), next) =>
      (MSetSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (MSetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: MSetRequest =>
      id == that.id &&
      values == that.values
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, values)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"MSetRequest($id, $values)"
}

object MSetRequest {

  def apply(id: UUID, values: Map[String, Any]): MSetRequest = new MSetRequest(id, values)

  def unapply(self: MSetRequest): Option[(UUID, Map[String, Any])] = Some((self.id, self.values))

  def create(id: UUID, values: Map[String, Any]): MSetRequest = apply(id, values)

}

sealed trait MSetResponse                                                    extends CommandResponse
final case class MSetSuspended(id: UUID, requestId: UUID)                    extends MSetResponse
final case class MSetSucceeded(id: UUID, requestId: UUID)                    extends MSetResponse
final case class MSetFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends MSetResponse
