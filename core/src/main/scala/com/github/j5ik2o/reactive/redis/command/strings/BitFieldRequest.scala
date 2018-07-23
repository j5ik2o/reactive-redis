package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command._
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model._
import fastparse.all._

case class BitFieldRequest(id: UUID, key: String, options: BitFieldRequest.SubOption*)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = BitFieldResponse

  override val isMasterOnly: Boolean = false

  override def asString: String = s"BITFIELD $key ${options.map(_.asString).mkString(" ")}"

  override protected def responseParser: P[Expr] = P(integerArrayReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (ArrayExpr(values), next) =>
      (BitFieldSucceeded(UUID.randomUUID(), id, values.asInstanceOf[Seq[NumberExpr]].map(_.value)), next)
    case (SimpleExpr(QUEUED), next) =>
      (BitFieldSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (BitFieldFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

object BitFieldRequest {

  sealed trait BitType {
    def asString: String
  }

  case class SingedBitType(bit: Int) extends BitType {
    override def asString: String = s"i$bit"
  }

  case class UnsignedBitType(bit: Int) extends BitType {
    override def asString: String = s"u$bit"
  }

  sealed trait SubOption {
    def asString: String
  }

  case class Get(bitType: BitType, offset: Int) extends SubOption {
    override def asString: String = s"GET ${bitType.asString} $offset"
  }

  case class Set(bitType: BitType, offset: Int, value: Int) extends SubOption {
    override def asString: String = s"SET ${bitType.asString} $offset $value"
  }

  case class IncrBy(bitType: BitType, offset: Int, increment: Int) extends SubOption {
    override def asString: String = s"INCRBY ${bitType.asString} $offset $increment"
  }

  sealed trait OverflowType {
    def asString: String
  }

  case object Wrap extends OverflowType {
    override def asString: String = "WRAP"
  }

  case object Sat extends OverflowType {
    override def asString: String = "SAT"
  }

  case object Fail extends OverflowType {
    override def asString: String = "FAIL"
  }

  case class Overflow(overflowType: OverflowType) extends SubOption {
    override def asString: String = s"OVERFLOW ${overflowType.asString}"
  }

}

sealed trait BitFieldResponse                                             extends CommandResponse
case class BitFieldSuspended(id: UUID, requestId: UUID)                   extends BitFieldResponse
case class BitFieldSucceeded(id: UUID, requestId: UUID, values: Seq[Int]) extends BitFieldResponse
case class BitFieldFailed(id: UUID, requestId: UUID, ex: Exception)       extends BitFieldResponse
