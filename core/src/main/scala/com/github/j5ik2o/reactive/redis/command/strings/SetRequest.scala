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

final case class SetRequest(id: UUID,
                            key: String,
                            value: String,
                            expiration: Option[SetExpiration],
                            setOption: Option[SetOption])
    extends CommandRequest
    with StringParsersSupport {

  override type Response = SetResponse

  override val isMasterOnly: Boolean = true

  private def setExpirationToString(expiration: SetExpiration): String = expiration match {
    case SetExExpiration(v) => v.toSeconds.toString
    case SetPxExpiration(v) => v.toMillis.toString
  }

  override def asString: String =
    s"""SET $key "$value"${expiration.fold("")(v => s" ${setExpirationToString(v)}")}${setOption.fold("")(
      v => s" ${v.entryName}"
    )}"""

  override protected lazy val responseParser: P[Expr] = fastParse(simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (SetSucceeded(UUID.randomUUID(), id), next)
    case (SimpleExpr(QUEUED), next) =>
      (SetSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (SetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

object SetRequest {

  def apply[A](id: UUID,
               key: String,
               value: A,
               expiration: Option[SetExpiration] = None,
               setOption: Option[SetOption] = None)(implicit s: Show[A]): SetRequest =
    new SetRequest(id, key, s.show(value), expiration, setOption)

}

sealed trait SetResponse                                                    extends CommandResponse
final case class SetSuspended(id: UUID, requestId: UUID)                    extends SetResponse
final case class SetSucceeded(id: UUID, requestId: UUID)                    extends SetResponse
final case class SetFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SetResponse
