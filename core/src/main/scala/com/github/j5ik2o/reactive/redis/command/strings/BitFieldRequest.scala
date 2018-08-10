package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command._
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model._
import fastparse.all._

final case class BitFieldRequest(id: UUID, key: String, options: BitFieldRequest.SubOption*)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = BitFieldResponse

  override val isMasterOnly: Boolean = false

  override def asString: String = cs("BITFIELD", Some(key) :: options.toSeq.flatMap(_.toSeq).map(Some(_)).toList: _*)

  override protected lazy val responseParser: P[Expr] = fastParse(
    integerArrayReply | simpleStringReply | errorReply
  )

  override protected lazy val parseResponse: Handler = {
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
    def toSeq: Seq[String]
  }

  final case class SingedBitType(bit: Int) extends BitType {
    override def toSeq: Seq[String] = Seq(s"i$bit")
  }

  final case class UnsignedBitType(bit: Int) extends BitType {
    override def toSeq: Seq[String] = Seq(s"u$bit")
  }

  sealed trait SubOption {
    def toSeq: Seq[String]
  }

  final case class Get(bitType: BitType, offset: Int) extends SubOption {
    override def toSeq: Seq[String] = Seq("GET") ++ bitType.toSeq ++ Seq(offset.toString)
  }

  final case class Set(bitType: BitType, offset: Int, value: Int) extends SubOption {
    override def toSeq: Seq[String] = Seq("SET") ++ bitType.toSeq ++ Seq(offset.toString, value.toString)
  }

  final case class IncrBy(bitType: BitType, offset: Int, increment: Int) extends SubOption {
    override def toSeq: Seq[String] = Seq("INCRBY") ++ bitType.toSeq ++ Seq(offset.toString, increment.toString)
  }

  sealed trait OverflowType {
    def toSeq: Seq[String]
  }

  case object Wrap extends OverflowType {
    override def toSeq: Seq[String] = Seq("WRAP")
  }

  case object Sat extends OverflowType {
    override def toSeq: Seq[String] = Seq("SAT")
  }

  case object Fail extends OverflowType {
    override def toSeq: Seq[String] = Seq("FAIL")
  }

  final case class Overflow(overflowType: OverflowType) extends SubOption {
    override def toSeq: Seq[String] = Seq("OVERFLOW") ++ overflowType.toSeq
  }

}

sealed trait BitFieldResponse                                                    extends CommandResponse
final case class BitFieldSuspended(id: UUID, requestId: UUID)                    extends BitFieldResponse
final case class BitFieldSucceeded(id: UUID, requestId: UUID, values: Seq[Long]) extends BitFieldResponse
final case class BitFieldFailed(id: UUID, requestId: UUID, ex: Exception)        extends BitFieldResponse
