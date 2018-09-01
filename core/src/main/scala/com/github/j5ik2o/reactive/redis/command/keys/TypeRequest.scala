package com.github.j5ik2o.reactive.redis.command.keys
import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import enumeratum._
import fastparse.all._

import scala.collection.immutable

sealed abstract class ValueType(override val entryName: String) extends EnumEntry

object ValueType extends Enum[ValueType] {
  override def values: immutable.IndexedSeq[ValueType] = findValues

  case object String extends ValueType("string")
  case object List   extends ValueType("list")
  case object Set    extends ValueType("set")
  case object ZSet   extends ValueType("zset")
  case object Hash   extends ValueType("hash")
  case object Stream extends ValueType("stream")

}

final class TypeRequest(val id: UUID, val key: String) extends CommandRequest with StringParsersSupport {

  override type Response = TypeResponse
  override val isMasterOnly: Boolean = true

  override def asString: String = cs("TYPE", Some(key))

  override protected lazy val responseParser: P[Expr] = fastParse(simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (SimpleExpr(QUEUED), next) =>
      (TypeSuspended(UUID.randomUUID(), id), next)
    case (SimpleExpr(typeName), next) =>
      (TypeSucceeded(UUID.randomUUID(), id, ValueType.withName(typeName)), next)
    case (ErrorExpr(msg), next) =>
      (TypeFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: TypeRequest =>
      id == that.id &&
      key == that.key
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"TypeRequest($id, $key)"

}

object TypeRequest {

  def apply(id: UUID, key: String): TypeRequest = new TypeRequest(id, key)

  def unapply(self: TtlRequest): Option[(UUID, String)] = Some((self.id, self.key))

  def create(id: UUID, key: String): TypeRequest = apply(id, key)

}

sealed trait TypeResponse                                                    extends CommandResponse
final case class TypeSucceeded(id: UUID, requestId: UUID, value: ValueType)  extends TypeResponse
final case class TypeSuspended(id: UUID, requestId: UUID)                    extends TypeResponse
final case class TypeFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends TypeResponse
