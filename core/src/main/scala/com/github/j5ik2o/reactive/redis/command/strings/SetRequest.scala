package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import cats.Show
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import enumeratum._
import fastparse.all._

import scala.collection.immutable
import scala.concurrent.duration.Duration

sealed trait SetExpiration { val duration: Duration }
final case class SetExExpiration(duration: Duration) extends SetExpiration
final case class SetPxExpiration(duration: Duration) extends SetExpiration
sealed trait SetOption                               extends EnumEntry
object SetOption extends Enum[SetOption] {
  override def values: immutable.IndexedSeq[SetOption] = findValues
  case object NX extends SetOption
  case object XX extends SetOption
}

final class SetRequest(val id: UUID,
                       val key: String,
                       val value: String,
                       val expiration: Option[SetExpiration],
                       val setOption: Option[SetOption])
    extends CommandRequest
    with StringParsersSupport {

  override type Response = SetResponse

  override val isMasterOnly: Boolean = true

  private def setExpirationToString(expiration: SetExpiration): String = expiration match {
    case SetExExpiration(v) => v.toSeconds.toString
    case SetPxExpiration(v) => v.toMillis.toString
  }

  override def asString: String =
    cs("SET", Some(key), Some(value), expiration.map(setExpirationToString), setOption.map(_.entryName))

  override protected lazy val responseParser: P[Expr] = fastParse(simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (SetSucceeded(UUID.randomUUID(), id), next)
    case (SimpleExpr(QUEUED), next) =>
      (SetSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (SetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: SetRequest =>
      id == that.id &&
      key == that.key &&
      value == that.value &&
      expiration == that.expiration &&
      setOption == that.setOption
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key, value, expiration, setOption)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"SetRequest($id, $key, $value, $expiration, $setOption)"

}

object SetRequest {

  def apply[A](id: UUID,
               key: String,
               value: A,
               expiration: Option[SetExpiration] = None,
               setOption: Option[SetOption] = None)(implicit s: Show[A]): SetRequest =
    new SetRequest(id, key, s.show(value), expiration, setOption)

  def unapply(self: SetRequest): Option[(UUID, String, String, Option[SetExpiration], Option[SetOption])] =
    Some((self.id, self.key, self.value, self.expiration, self.setOption))

  def create[A](id: UUID,
                key: String,
                value: A,
                expiration: Option[SetExpiration],
                setOption: Option[SetOption],
                s: Show[A]): SetRequest =
    new SetRequest(id, key, s.show(value), expiration, setOption)

}

sealed trait SetResponse                                                    extends CommandResponse
final case class SetSuspended(id: UUID, requestId: UUID)                    extends SetResponse
final case class SetSucceeded(id: UUID, requestId: UUID)                    extends SetResponse
final case class SetFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SetResponse
