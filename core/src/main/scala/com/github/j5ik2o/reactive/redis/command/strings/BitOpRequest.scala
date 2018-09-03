package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command._
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import enumeratum._
import fastparse.all._

import scala.collection.immutable

final class BitOpRequest(val id: UUID,
                         val operand: BitOpRequest.Operand,
                         val outputKey: String,
                         val inputKey1: String,
                         val inputKey2: String)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = BitOpResponse

  override val isMasterOnly: Boolean = true

  override def asString: String =
    cs("BITOP", Some(operand.entryName), Some(outputKey), Some(inputKey1), Some(inputKey2))

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (BitOpSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (BitOpSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (BitOpFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: BitOpRequest =>
      id == that.id &&
      operand == that.operand &&
      outputKey == that.outputKey &&
      inputKey1 == that.inputKey1 &&
      inputKey2 == that.inputKey2
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, operand, outputKey, inputKey1, inputKey2)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"BitOpRequest($id, $operand, $outputKey, $inputKey1, $inputKey2)"
}

object BitOpRequest {

  def apply(id: UUID,
            operand: BitOpRequest.Operand,
            outputKey: String,
            inputKey1: String,
            inputKey2: String): BitOpRequest =
    new BitOpRequest(id, operand, outputKey, inputKey1, inputKey2)

  def unapply(self: BitOpRequest): Option[(UUID, Operand, String, String, String)] =
    Some((self.id, self.operand, self.outputKey, self.inputKey1, self.inputKey2))

  def create(id: UUID,
             operand: BitOpRequest.Operand,
             outputKey: String,
             inputKey1: String,
             inputKey2: String): BitOpRequest = apply(id, operand, outputKey, inputKey1, inputKey2)

  sealed abstract class Operand(override val entryName: String) extends EnumEntry

  object Operand extends Enum[Operand] {

    override def values: immutable.IndexedSeq[Operand] = findValues

    case object AND extends Operand("AND")

    case object OR extends Operand("OR")

    case object XOR extends Operand("XOR")

  }

}

sealed trait BitOpResponse                                              extends CommandResponse
final case class BitOpSuspended(id: UUID, requestId: UUID)              extends BitOpResponse
final case class BitOpSucceeded(id: UUID, requestId: UUID, value: Long) extends BitOpResponse
final case class BitOpFailed(id: UUID, requestId: UUID, ex: Exception)  extends BitOpResponse
