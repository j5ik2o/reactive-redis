package com.github.j5ik2o.reactive.redis

import java.text.ParseException
import java.util.UUID

import com.github.j5ik2o.reactive.redis.CommandResponseParser._

import scala.concurrent.duration.{ Duration, FiniteDuration }

object StringsOperations {

  import Options._

  // --- APPEND

  object AppendRequest extends SimpleResponseFactory {

    override protected val responseParser: Parser[Expr] = numberWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (NumberExpr(n), next) =>
        (AppendSucceeded(UUID.randomUUID(), requestId, n), next)
      case (SimpleExpr("QUEUED"), next) =>
        (AppendSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (AppendFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("APPEND request = {}", expr)
        throw new ParseException("APPEND request", o.offset)
    }

  }

  case class AppendRequest(id: UUID, key: String, value: String) extends SimpleRequest {
    override val responseFactory: SimpleResponseFactory = AppendRequest
    override val message: String                        = s"APPEND $key $value"
  }

  sealed trait AppendResponse extends Response

  case class AppendSuspended(id: UUID, requestId: UUID) extends AppendResponse

  case class AppendSucceeded(id: UUID, requestId: UUID, value: Int) extends AppendResponse

  case class AppendFailed(id: UUID, requestId: UUID, ex: Exception) extends AppendResponse

  // --- BITCOUNT

  object BitCountRequest extends SimpleResponseFactory {

    override protected val responseParser: Parser[Expr] = numberWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (NumberExpr(n), next) =>
        (BitCountSucceeded(UUID.randomUUID(), requestId, n), next)
      case (SimpleExpr("QUEUED"), next) =>
        (BitCountSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (BitCountFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("BITCOUNT request = {}", expr)
        throw new ParseException("BITCOUNT request", o.offset)
    }

  }

  case class BitCountRequest(id: UUID, key: String, startAndEnd: Option[StartAndEnd] = None) extends SimpleRequest {
    override val responseFactory: SimpleResponseFactory = BitCountRequest
    override val message: String                        = s"BITCOUNT $key" + startAndEnd.fold("")(e => " " + e.start + " " + e.end)
  }

  sealed trait BitCountResponse extends Response

  case class BitCountSuspended(id: UUID, requestId: UUID) extends BitCountResponse

  case class BitCountSucceeded(id: UUID, requestId: UUID, value: Int) extends BitCountResponse

  case class BitCountFailed(id: UUID, requestId: UUID, ex: Exception) extends BitCountResponse

  // --- BITFIELD
  object BitFieldRequest extends SimpleResponseFactory {

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

    override protected val responseParser: Parser[Expr] =
      numberArrayWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (ArrayExpr(values), next) =>
        val _values = values.asInstanceOf[Seq[NumberExpr]]
        (BitFieldSucceeded(UUID.randomUUID(), requestId, _values.map(_.value)), next)
      case (SimpleExpr("QUEUED"), next) =>
        (BitFieldSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (BitFieldFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("BITFIELD request = {}", expr)
        throw new ParseException("BITFIELD request", o.offset)
    }

  }

  case class BitFieldRequest(id: UUID, key: String, options: BitFieldRequest.SubOption*) extends SimpleRequest {
    override val responseFactory: SimpleResponseFactory = BitFieldRequest
    override val message: String = {
      s"BITFIELD $key ${options.map(_.asString).mkString(" ")}"
    }
  }

  sealed trait BitFieldResponse extends Response

  case class BitFieldSuspended(id: UUID, requestId: UUID) extends BitFieldResponse

  case class BitFieldSucceeded(id: UUID, requestId: UUID, values: Seq[Int]) extends BitFieldResponse

  case class BitFieldFailed(id: UUID, requestId: UUID, ex: Exception) extends BitFieldResponse

  // --- BITOP
  object BitOpRequest extends SimpleResponseFactory {

    sealed trait Operand

    object Operand {

      case object AND extends Operand

      case object OR extends Operand

      case object XOR extends Operand

    }

    override protected val responseParser: Parser[Expr] = numberWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (NumberExpr(n), next) =>
        (BitOpSucceeded(UUID.randomUUID(), requestId, n), next)
      case (SimpleExpr("QUEUED"), next) =>
        (BitOpSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (BitOpFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("BITOP request = {}", expr)
        throw new ParseException("BITOP request", o.offset)
    }

  }

  case class BitOpRequest(id: UUID,
                          operand: BitOpRequest.Operand,
                          outputKey: String,
                          inputKey1: String,
                          inputKey2: String)
      extends SimpleRequest {
    override val responseFactory: SimpleResponseFactory = BitOpRequest
    override val message: String                        = s"BITOP ${operand.toString} $outputKey $inputKey1 $inputKey2"
  }

  sealed trait BitOpResponse extends Response

  case class BitOpSuspended(id: UUID, requestId: UUID) extends BitOpResponse

  case class BitOpSucceeded(id: UUID, requestId: UUID, value: Int) extends BitOpResponse

  case class BitOpFailed(id: UUID, requestId: UUID, ex: Exception) extends BitOpResponse

  // --- BITPOS
  object BitPosRequest extends SimpleResponseFactory {

    override protected val responseParser: Parser[Expr] = numberWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (NumberExpr(n), next) =>
        (BitPosSucceeded(UUID.randomUUID(), requestId, n), next)
      case (SimpleExpr("QUEUED"), next) =>
        (BitPosSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (BitPosFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("BITPOS request = {}", expr)
        throw new ParseException("BITPOS request", o.offset)
    }

  }

  case class BitPosRequest(id: UUID, key: String, bit: Int, startAndEnd: Option[StartAndEnd] = None)
      extends SimpleRequest {
    override val responseFactory: SimpleResponseFactory = BitPosRequest
    override val message: String                        = s"BITPOS $key $bit" + startAndEnd.fold("")(e => " " + e.start + " " + e.end)
  }

  sealed trait BitPosResponse extends Response

  case class BitPosSuspended(id: UUID, requestId: UUID) extends BitPosResponse

  case class BitPosSucceeded(id: UUID, requestId: UUID, value: Int) extends BitPosResponse

  case class BitPosFailed(id: UUID, requestId: UUID, ex: Exception) extends BitPosResponse

  // --- DECR
  object DecrRequest extends SimpleResponseFactory {

    override protected val responseParser: DecrRequest.Parser[Expr] = numberWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (NumberExpr(n), next) =>
        (DecrSucceeded(UUID.randomUUID(), requestId, n), next)
      case (SimpleExpr("QUEUED"), next) =>
        (DecrSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (DecrFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("DECR request = {}", expr)
        throw new ParseException("DECR request", o.offset)
    }

  }

  case class DecrRequest(id: UUID, key: String) extends SimpleRequest {
    override val responseFactory: SimpleResponseFactory = DecrRequest
    override val message: String                        = s"DECR $key"
  }

  sealed trait DecrResponse extends Response

  case class DecrSuspended(id: UUID, requestId: UUID) extends DecrResponse

  case class DecrSucceeded(id: UUID, requestId: UUID, value: Int) extends DecrResponse

  case class DecrFailed(id: UUID, requestId: UUID, ex: Exception) extends DecrResponse

  // --- DECRBY
  object DecrByRequest extends SimpleResponseFactory {

    override protected val responseParser: DecrByRequest.Parser[Expr] =
      numberWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (NumberExpr(n), next) =>
        (DecrBySucceeded(UUID.randomUUID(), requestId, n), next)
      case (SimpleExpr("QUEUED"), next) =>
        (DecrBySuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (DecrByFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("DECRBY request = {}", expr)
        throw new ParseException("DECRBY request", o.offset)
    }

  }

  case class DecrByRequest(id: UUID, key: String, value: Int) extends SimpleRequest {
    override val responseFactory: SimpleResponseFactory = DecrByRequest
    override val message: String                        = s"DECRBY $key $value"
  }

  sealed trait DecrByResponse extends Response

  case class DecrBySuspended(id: UUID, requestId: UUID) extends DecrByResponse

  case class DecrBySucceeded(id: UUID, requestId: UUID, value: Int) extends DecrByResponse

  case class DecrByFailed(id: UUID, requestId: UUID, ex: Exception) extends DecrByResponse

  // --- GET
  object GetRequest extends SimpleResponseFactory {

    override protected val responseParser
      : Parser[Expr] = simpleWithCrLfOrErrorWithCrLf | bulkStringWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (StringOptExpr(s), next) =>
        (GetSucceeded(UUID.randomUUID(), requestId, s), next)
      case (SimpleExpr("QUEUED"), next) =>
        (GetSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (GetFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("GET request = {}", expr)
        throw new ParseException("GET request", o.offset)
    }

  }

  case class GetRequest(id: UUID, key: String) extends SimpleRequest {
    override val message: String                        = s"GET $key"
    override val responseFactory: SimpleResponseFactory = GetRequest
  }

  sealed trait GetResponse extends Response

  case class GetSuspended(id: UUID, requestId: UUID) extends GetResponse

  case class GetSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends GetResponse

  case class GetFailed(id: UUID, requestId: UUID, ex: Exception) extends GetResponse

  // --- GETBIT
  object GetBitRequest extends SimpleResponseFactory {

    override protected val responseParser: Parser[Expr] = numberWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (NumberExpr(n), next) =>
        (GetBitSucceeded(UUID.randomUUID(), requestId, n), next)
      case (SimpleExpr("QUEUED"), next) =>
        (GetBitSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (GetBitFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("GETBIT request = {}", expr)
        throw new ParseException("GETBIT request", o.offset)
    }

  }

  case class GetBitRequest(id: UUID, key: String, offset: Int) extends SimpleRequest {
    override val message: String                        = s"GETBIT $key $offset"
    override val responseFactory: SimpleResponseFactory = GetBitRequest
  }

  sealed trait GetBitResponse extends Response

  case class GetBitSuspended(id: UUID, requestId: UUID) extends GetBitResponse

  case class GetBitSucceeded(id: UUID, requestId: UUID, value: Int) extends GetBitResponse

  case class GetBitFailed(id: UUID, requestId: UUID, ex: Exception) extends GetBitResponse

  // --- GETRANGE
  object GetRangeRequest extends SimpleResponseFactory {

    override protected val responseParser
      : Parser[Expr] = simpleWithCrLfOrErrorWithCrLf | bulkStringWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (StringOptExpr(s), next) =>
        (GetRangeSucceeded(UUID.randomUUID(), requestId, s), next)
      case (SimpleExpr("QUEUED"), next) =>
        (GetRangeSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (GetRangeFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("GETRANGE request = {}", expr)
        throw new ParseException("GETRANGE request", o.offset)
    }

  }

  case class GetRangeRequest(id: UUID, key: String, startAndEnd: StartAndEnd) extends SimpleRequest {
    override val message: String                        = s"GETRANGE $key ${startAndEnd.start} ${startAndEnd.end}"
    override val responseFactory: SimpleResponseFactory = GetRangeRequest
  }

  sealed trait GetRangeResponse extends Response

  case class GetRangeSuspended(id: UUID, requestId: UUID) extends GetRangeResponse

  case class GetRangeSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends GetRangeResponse

  case class GetRangeFailed(id: UUID, requestId: UUID, ex: Exception) extends GetRangeResponse

  // --- GETSET
  object GetSetResponse extends SimpleResponseFactory {

    override protected val responseParser
      : Parser[Expr] = simpleWithCrLfOrErrorWithCrLf | bulkStringWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (StringOptExpr(s), next) =>
        (GetSetSucceeded(UUID.randomUUID(), requestId, s), next)
      case (SimpleExpr("QUEUED"), next) =>
        (GetSetSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (GetSetFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("GETSET request = {}", expr)
        throw new ParseException("GETSET request", o.offset)
    }

  }

  case class GetSetRequest(id: UUID, key: String, value: String) extends SimpleRequest {
    override val message: String                        = s"GETSET $key $value"
    override val responseFactory: SimpleResponseFactory = GetSetResponse
  }

  sealed trait GetSetResponse extends Response

  case class GetSetSuspended(id: UUID, requestId: UUID) extends GetSetResponse

  case class GetSetSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends GetSetResponse

  case class GetSetFailed(id: UUID, requestId: UUID, ex: Exception) extends GetSetResponse

  // --- INCR
  object IncrRequest extends SimpleResponseFactory {

    override protected val responseParser: Parser[Expr] = numberWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (NumberExpr(n), next) =>
        (IncrSucceeded(UUID.randomUUID(), requestId, n), next)
      case (SimpleExpr("QUEUED"), next) =>
        (IncrSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (IncrFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("INCR request = {}", expr)
        throw new ParseException("INCR request", o.offset)
    }

  }

  case class IncrRequest(id: UUID, key: String) extends SimpleRequest {
    override val responseFactory: SimpleResponseFactory = IncrRequest
    override val message: String                        = s"INCR $key"
  }

  sealed trait IncrResponse extends Response

  case class IncrSuspended(id: UUID, requestId: UUID) extends IncrResponse

  case class IncrSucceeded(id: UUID, requestId: UUID, value: Int) extends IncrResponse

  case class IncrFailed(id: UUID, requestId: UUID, ex: Exception) extends IncrResponse

  // --- INCRBY
  object IncrByRequest extends SimpleResponseFactory {

    override protected val responseParser: Parser[Expr] = numberWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (NumberExpr(n), next) =>
        (IncrBySucceeded(UUID.randomUUID(), requestId, n), next)
      case (SimpleExpr("QUEUED"), next) =>
        (IncrBySuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (IncrByFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("INCRBY request = {}", expr)
        throw new ParseException("INCRBY request", o.offset)
    }

  }

  case class IncrByRequest(id: UUID, key: String, value: Int) extends SimpleRequest {
    override val responseFactory: SimpleResponseFactory = IncrByRequest
    override val message: String                        = s"INCRBY $key $value"
  }

  sealed trait IncrByResponse extends Response

  case class IncrBySuspended(id: UUID, requestId: UUID) extends IncrByResponse

  case class IncrBySucceeded(id: UUID, requestId: UUID, value: Int) extends IncrByResponse

  case class IncrByFailed(id: UUID, requestId: UUID, ex: Exception) extends IncrByResponse

  // --- INCRBYFLOAT
  object IncrByFloatRequest extends SimpleResponseFactory {

    override protected val responseParser: Parser[Expr] = bulkStringWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (StringOptExpr(s), next) =>
        (IncrByFloatSucceeded(UUID.randomUUID(), requestId, s.get.toDouble), next)
      case (SimpleExpr("QUEUED"), next) =>
        (IncrByFloatSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (IncrByFloatFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("INCRBYFLOAT request = {}", expr)
        throw new ParseException("INCRBYFLOAT request", o.offset)
    }

  }
  case class IncrByFloatRequest(id: UUID, key: String, value: Double) extends SimpleRequest {
    override val responseFactory: SimpleResponseFactory = IncrByFloatRequest
    override val message: String                        = s"INCRBYFLOAT $key $value"
  }

  sealed trait IncrByFloatResponse extends Response

  case class IncrByFloatSuspended(id: UUID, requestId: UUID) extends IncrByFloatResponse

  case class IncrByFloatSucceeded(id: UUID, requestId: UUID, value: Double) extends IncrByFloatResponse

  case class IncrByFloatFailed(id: UUID, requestId: UUID, ex: Exception) extends IncrByFloatResponse

  // --- MGET
  object MGetRequest extends SimpleResponseFactory {

    override protected val responseParser: Parser[Expr] = stringOptArrayWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (ArrayExpr(values), next) =>
        val _values = values.asInstanceOf[Seq[StringOptExpr]]
        (MGetSucceeded(UUID.randomUUID(), requestId, _values.map(_.value)), next)
      case (SimpleExpr("QUEUED"), next) =>
        (MGetSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (MGetFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("MGET request = {}", expr)
        throw new ParseException("MGET request", o.offset)
    }

  }

  case class MGetRequest(id: UUID, keys: Seq[String]) extends SimpleRequest {
    override val message: String                        = s"MGET ${keys.mkString(" ")}"
    override val responseFactory: SimpleResponseFactory = MGetRequest
  }

  sealed trait MGetResponse extends Response

  case class MGetSuspended(id: UUID, requestId: UUID) extends MGetResponse

  case class MGetSucceeded(id: UUID, requestId: UUID, values: Seq[Option[String]]) extends MGetResponse

  case class MGetFailed(id: UUID, requestId: UUID, ex: Exception) extends MGetResponse

  // --- MSET
  object MSetRequest extends SimpleResponseFactory {

    override protected val responseParser: Parser[Expr] = simpleWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (SimpleExpr("OK"), next) =>
        (MSetSucceeded(UUID.randomUUID(), requestId), next)
      case (SimpleExpr("QUEUED"), next) =>
        (MSetSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (MSetFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("MSET request = {}", expr)
        throw new ParseException("MSET request", o.offset)
    }

  }

  case class MSetRequest private (id: UUID, values: Map[String, Any]) extends SimpleRequest {
    override val message: String = {
      val keyWithValues = values.foldLeft("") {
        case (r, (k, v)) =>
          r + s""" $k "$v""""
      }
      s"MSET $keyWithValues"
    }
    override val responseFactory: SimpleResponseFactory = MSetRequest
  }

  sealed trait MSetResponse extends Response

  case class MSetSuspended(id: UUID, requestId: UUID) extends MSetResponse

  case class MSetSucceeded(id: UUID, requestId: UUID) extends MSetResponse

  case class MSetFailed(id: UUID, requestId: UUID, ex: Exception) extends MSetResponse

  // --- MSETNX
  object MSetNxRequest extends SimpleResponseFactory {

    override protected val responseParser: Parser[Expr] = numberWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (NumberExpr(n), next) =>
        (MSetNxSucceeded(UUID.randomUUID(), requestId, n == 1), next)
      case (SimpleExpr("QUEUED"), next) =>
        (MSetNxSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (MSetNxFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("MSETNX request = {}", expr)
        throw new ParseException("MSETNX request", o.offset)
    }

  }

  case class MSetNxRequest(id: UUID, values: Map[String, Any]) extends SimpleRequest {
    override val message: String = {
      val keyWithValues = values.foldLeft("") {
        case (r, (k, v)) =>
          r + s""" $k "$v""""
      }
      s"MSETNX $keyWithValues"
    }
    override val responseFactory: SimpleResponseFactory = MSetNxRequest
  }

  sealed trait MSetNxResponse extends Response

  case class MSetNxSuspended(id: UUID, requestId: UUID) extends MSetNxResponse

  case class MSetNxSucceeded(id: UUID, requestId: UUID, isSet: Boolean) extends MSetNxResponse

  case class MSetNxFailed(id: UUID, requestId: UUID, ex: Exception) extends MSetNxResponse

  // --- PSETEX
  object PSetExRequest extends SimpleResponseFactory {

    override protected val responseParser: Parser[Expr] = simpleWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (SimpleExpr("OK"), next) =>
        (PSetExSucceeded(UUID.randomUUID(), requestId), next)
      case (SimpleExpr("QUEUED"), next) =>
        (PSetExSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (PSetExFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("PSETEX request = {}", expr)
        throw new ParseException("PSETEX request", o.offset)
    }

    def apply(id: UUID, key: String, duration: FiniteDuration, value: Long): PSetExRequest =
      new PSetExRequest(id, key, duration, value.toString)
    def apply(id: UUID, key: String, duration: FiniteDuration, value: Int): PSetExRequest =
      new PSetExRequest(id, key, duration, value.toString)
    def apply(id: UUID, key: String, duration: FiniteDuration, value: Double): PSetExRequest =
      new PSetExRequest(id, key, duration, value.toString)
    def apply(id: UUID, key: String, duration: FiniteDuration, value: Float): PSetExRequest =
      new PSetExRequest(id, key, duration, value.toString)
    def apply(id: UUID, key: String, duration: FiniteDuration, value: BigDecimal): PSetExRequest =
      new PSetExRequest(id, key, duration, value.toString)

  }

  case class PSetExRequest private (id: UUID, key: String, millis: FiniteDuration, value: String)
      extends SimpleRequest {
    override val message: String                        = s"""PSETEX $key ${millis.toMillis} "$value""""
    override val responseFactory: SimpleResponseFactory = PSetExRequest
  }

  sealed trait PSetExResponse extends Response

  case class PSetExSuspended(id: UUID, requestId: UUID) extends PSetExResponse

  case class PSetExSucceeded(id: UUID, requestId: UUID) extends PSetExResponse

  case class PSetExFailed(id: UUID, requestId: UUID, ex: Exception) extends PSetExResponse

  // --- SET
  object SetRequest extends SimpleResponseFactory {

    override protected val responseParser: Parser[Expr] = simpleWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (SimpleExpr("OK"), next) =>
        (SetSucceeded(UUID.randomUUID(), requestId), next)
      case (SimpleExpr("QUEUED"), next) =>
        (SetSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (SetFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("SET request = {}", expr)
        throw new ParseException("SET request", o.offset)
    }

    def apply(id: UUID, key: String, value: Long): SetRequest       = new SetRequest(id, key, value.toString)
    def apply(id: UUID, key: String, value: Int): SetRequest        = new SetRequest(id, key, value.toString)
    def apply(id: UUID, key: String, value: Double): SetRequest     = new SetRequest(id, key, value.toString)
    def apply(id: UUID, key: String, value: Float): SetRequest      = new SetRequest(id, key, value.toString)
    def apply(id: UUID, key: String, value: BigDecimal): SetRequest = new SetRequest(id, key, value.toString)

  }

  case class SetRequest private (id: UUID, key: String, value: String) extends SimpleRequest {
    override val message: String                        = s"""SET $key "$value""""
    override val responseFactory: SimpleResponseFactory = SetRequest
  }

  sealed trait SetResponse extends Response

  case class SetSuspended(id: UUID, requestId: UUID) extends SetResponse

  case class SetSucceeded(id: UUID, requestId: UUID) extends SetResponse

  case class SetFailed(id: UUID, requestId: UUID, ex: Exception) extends SetResponse

  // --- SETBIT
  object SetBitRequest extends SimpleResponseFactory {

    override protected val responseParser: Parser[Expr] = numberWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (NumberExpr(n), next) =>
        (SetBitSucceeded(UUID.randomUUID(), requestId, n), next)
      case (SimpleExpr("QUEUED"), next) =>
        (SetBitSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (SetBitFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("SETBIT request = {}", expr)
        throw new ParseException("SETBIT request", o.offset)
    }

  }

  case class SetBitRequest private (id: UUID, key: String, offset: Int, value: Int) extends SimpleRequest {
    override val message: String                        = s"""SETBIT $key $offset $value"""
    override val responseFactory: SimpleResponseFactory = SetBitRequest
  }

  sealed trait SetBitResponse extends Response

  case class SetBitSuspended(id: UUID, requestId: UUID) extends SetBitResponse

  case class SetBitSucceeded(id: UUID, requestId: UUID, value: Int) extends SetBitResponse

  case class SetBitFailed(id: UUID, requestId: UUID, ex: Exception) extends SetBitResponse

  // --- SETEX
  object SetExRequest extends SimpleResponseFactory {
    override def receive(requestId: UUID): Handler = {
      case (SimpleExpr("OK"), next) =>
        (SetExSucceeded(UUID.randomUUID(), requestId), next)
      case (SimpleExpr("QUEUED"), next) =>
        (SetExSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (SetExFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("SETEX request = {}", expr)
        throw new ParseException("SETEX request", o.offset)
    }

    override protected val responseParser: Parser[Expr] = simpleWithCrLfOrErrorWithCrLf
  }

  case class SetExRequest(id: UUID, key: String, expires: FiniteDuration, value: String) extends SimpleRequest {
    override val message: String                        = s"SETEX $key ${expires.toSeconds} $value"
    override val responseFactory: SimpleResponseFactory = SetExRequest
  }

  sealed trait SetExResponse extends Response

  case class SetExSuspended(id: UUID, requestId: UUID) extends SetExResponse

  case class SetExSucceeded(id: UUID, requestId: UUID) extends SetExResponse

  case class SetExFailed(id: UUID, requestId: UUID, ex: Exception) extends SetExResponse

  // --- SETNX
  object SetNxRequest extends SimpleResponseFactory {

    override protected val responseParser: Parser[Expr] = numberWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (NumberExpr(n), next) =>
        (SetNxSucceeded(UUID.randomUUID(), requestId, n == 1), next)
      case (SimpleExpr("QUEUED"), next) =>
        (SetNxSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (SetNxFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("SETNX request = {}", expr)
        throw new ParseException("SETNX request", o.offset)
    }

  }

  case class SetNxRequest(id: UUID, key: String, value: String) extends SimpleRequest {
    override val message: String                        = s"SETNX $key $value"
    override val responseFactory: SimpleResponseFactory = SetNxRequest
  }

  sealed trait SetNxResponse extends Response

  case class SetNxSuspended(id: UUID, requestId: UUID) extends SetNxResponse

  case class SetNxSucceeded(id: UUID, requestId: UUID, isSet: Boolean) extends SetNxResponse

  case class SetNxFailed(id: UUID, requestId: UUID, ex: Exception) extends SetNxResponse

  // --- SETRANGE
  object SetRangeRequest extends SimpleResponseFactory {
    override def receive(requestId: UUID): Handler = {
      case (NumberExpr(n), next) =>
        (SetRangeSucceeded(UUID.randomUUID(), requestId, n), next)
      case (SimpleExpr("QUEUED"), next) =>
        (SetRangeSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (SetRangeFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("SETRANGE request = {}", expr)
        throw new ParseException("SETRANGE request", o.offset)
    }

    override protected val responseParser: Parser[Expr] = numberWithCrLfOrErrorWithCrLf
  }

  case class SetRangeRequest(id: UUID, key: String, range: Int, value: String) extends SimpleRequest {
    override val message: String                        = s"SETRANGE $key $range $value"
    override val responseFactory: SimpleResponseFactory = SetRangeRequest
  }

  sealed trait SetRangeResponse extends Response

  case class SetRangeSuspended(id: UUID, requestId: UUID) extends SetRangeResponse

  case class SetRangeSucceeded(id: UUID, requestId: UUID, value: Int) extends SetRangeResponse

  case class SetRangeFailed(id: UUID, requestId: UUID, ex: Exception) extends SetRangeResponse

  // --- STRLEN
  object StrLenRequest extends SimpleResponseFactory {

    override protected val responseParser: Parser[Expr] = numberWithCrLfOrErrorWithCrLf

    override def receive(requestId: UUID): Handler = {
      case (NumberExpr(n), next) =>
        (StrLenSucceeded(UUID.randomUUID(), requestId, n), next)
      case (SimpleExpr("QUEUED"), next) =>
        (StrLenSuspended(UUID.randomUUID(), requestId), next)
      case (ErrorExpr(msg), next) =>
        (StrLenFailed(UUID.randomUUID(), requestId, new Exception(msg)), next)
      case (expr, o) =>
        logger.error("STRLEN request = {}", expr)
        throw new ParseException("STRLEN request", o.offset)
    }

  }

  case class StrLenRequest(id: UUID, key: String) extends SimpleRequest {
    override val message: String                        = s"STRLEN $key"
    override val responseFactory: SimpleResponseFactory = StrLenRequest
  }

  sealed trait StrLenResponse extends Response

  case class StrLenSuspended(id: UUID, requestId: UUID) extends StrLenResponse

  case class StrLenSucceeded(id: UUID, requestId: UUID, length: Int) extends StrLenResponse

  case class StrLenFailed(id: UUID, requestId: UUID, ex: Exception) extends StrLenResponse

}
