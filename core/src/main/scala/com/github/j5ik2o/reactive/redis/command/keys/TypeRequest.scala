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

final case class TypeRequest(id: UUID, key: String) extends CommandRequest with StringParsersSupport {

  override type Response = TypeResponse
  override val isMasterOnly: Boolean = true

  override def asString: String = cs("TYPE", Some(key))

  override protected def responseParser: P[Expr] = fastParse(simpleStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (SimpleExpr(QUEUED), next) =>
      (TypeSuspended(UUID.randomUUID(), id), next)
    case (SimpleExpr(typeName), next) =>
      (TypeSucceeded(UUID.randomUUID(), id, ValueType.withName(typeName)), next)
    case (ErrorExpr(msg), next) =>
      (TypeFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait TypeResponse                                                    extends CommandResponse
final case class TypeSucceeded(id: UUID, requestId: UUID, value: ValueType)  extends TypeResponse
final case class TypeSuspended(id: UUID, requestId: UUID)                    extends TypeResponse
final case class TypeFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends TypeResponse
