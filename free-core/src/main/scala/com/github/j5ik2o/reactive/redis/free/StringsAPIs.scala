package com.github.j5ik2o.reactive.redis.free

import com.github.j5ik2o.reactive.redis.Options.StartAndEnd
import com.github.j5ik2o.reactive.redis.StringsOperations.{ BitFieldRequest, BitOpRequest }

import scala.concurrent.duration.FiniteDuration

trait StringsAPIs {

  sealed trait StringsDSL[+A] extends RedisDSL[A]

  // --- APPEND

  case class Append(key: String, value: String) extends StringsDSL[Option[Int]]

  // --- BITCOUNT

  case class BitCount(key: String, startAndEnd: Option[StartAndEnd] = None) extends StringsDSL[Option[Int]]

  // --- BITFIELD

  case class BitField(key: String, options: BitFieldRequest.SubOption*) extends StringsDSL[Option[Seq[Int]]]

  // --- BITOP

  case class BitOp(operand: BitOpRequest.Operand, outputKey: String, inputKey1: String, inputKey2: String)
      extends StringsDSL[Option[Int]]

  // --- BITPOS

  case class BitPos(key: String, bit: Int, startAndEnd: Option[StartAndEnd] = None) extends StringsDSL[Option[Int]]

  // --- DECR

  case class Descr(key: String) extends StringsDSL[Option[Int]]

  // --- DECRBY

  case class DescrBy(key: String, value: Int) extends StringsDSL[Option[Int]]

  // --- GET

  case class Get(key: String) extends StringsDSL[Option[String]]

  // --- GETBIT

  case class GetBit(key: String, offset: Int) extends StringsDSL[Option[Int]]

  // --- GETRANGE

  case class GetRange(key: String, startAndEnd: StartAndEnd) extends StringsDSL[Option[String]]

  // --- GETSET

  case class GetSet(key: String, value: String) extends StringsDSL[Option[String]]

  // --- INCR

  case class Incr(key: String) extends StringsDSL[Option[Int]]

  // --- INCRBY

  case class IncrBy(key: String, value: Int) extends StringsDSL[Option[Int]]

  // --- INCRBYFLOAT

  case class IncrByFloat(key: String, value: Double) extends StringsDSL[Option[Double]]

  // --- MGET

  case class MGet(keys: Seq[String]) extends StringsDSL[Option[Seq[Option[String]]]]

  // --- MSET

  case class MSet(values: Map[String, Any]) extends StringsDSL[Unit]

  // --- MSETNX

  case class MSetNx(values: Map[String, Any]) extends StringsDSL[Option[Boolean]]

  // --- PSETEX

  case class PSetEx(key: String, duration: FiniteDuration, value: String) extends StringsDSL[Unit]

  // --- SET

  case class Set(key: String, value: String) extends StringsDSL[Unit]

  // --- SETBIT

  case class SetBit(key: String, offset: Int, value: Int) extends StringsDSL[Option[Int]]

  // --- SETEX

  case class SetEx(key: String, expires: FiniteDuration, value: String) extends StringsDSL[Unit]

  // --- SETNX

  case class SetNx(key: String, value: String) extends StringsDSL[Option[Boolean]]

  // --- SETRANGE

  case class SetRange(key: String, range: Int, value: String) extends StringsDSL[Option[Int]]

  // --- STRLEN

  case class StrLen(key: String) extends StringsDSL[Option[Int]]

}
