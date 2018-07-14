package com.github.j5ik2o.reactive.redis.parser

import com.github.j5ik2o.reactive.redis.parser.model._
import fastparse.byte.all._

object ByteParsers {
  val crlf: P0 = P(BS('\r'.toByte, '\n'.toByte))

  val digit: P0      = P(ByteIn(('0' to '9').map(_.toByte)))
  val lowerAlpha: P0 = P(ByteIn(('a' to 'z').map(_.toByte)))
  val upperAlpha: P0 = P(ByteIn(('A' to 'Z').map(_.toByte)))
  val alpha: P0      = P(lowerAlpha | upperAlpha)
  val alphaDigit: P0 = P(alpha | digit)

  val error: P[ErrorExpr] = P(BS('-'.toByte) ~ (!crlf ~/ AnyByte).rep(1).!).map(v => ErrorExpr(v.decodeUtf8.right.get))

  val length = P(BS('$'.toByte) ~ BS('-'.toByte).!.? ~ digit.rep(1).!).map {
    case (m, n) =>
      val r = LengthExpr(m.map(_ => -1).getOrElse(1) * n.decodeUtf8.right.get.toInt)
      println(r)
      r
  }
  val simple: P[SimpleExpr] = P(BS('+'.toByte) ~ alphaDigit.rep(1).!).map(v => SimpleExpr(v.decodeUtf8.right.get))
  val number: P[NumberExpr] = P(BS(':'.toByte) ~ BS('-'.toByte).!.? ~ alphaDigit.rep(1).!).map {
    case (minus, n) =>
      NumberExpr(minus.map(_ => -1).getOrElse(1) * n.decodeUtf8.right.get.toInt)
  }
  val byteString: P[BytesExpr] = P((!crlf ~/ AnyByte).rep.!).map(v => BytesExpr(v.toArray))
  val string: P[StringExpr]    = P((!crlf ~/ AnyByte).rep.!).map(v => StringExpr(v.decodeUtf8.right.get))
  val arrayPrefix: P[Int]      = P(BS('*'.toByte) ~ digit.rep(1).!).map(_.decodeUtf8.right.get.toInt)

  val errorWithCrLf: P[ErrorExpr] = P(error ~ crlf)

  val simpleWithCrLf: P[SimpleExpr]         = P(simple ~ crlf)
  val integerWithCrLf: P[NumberExpr]        = P(number ~ crlf)
  val arrayPrefixWithCrlf: P[ArraySizeExpr] = P(arrayPrefix ~ crlf).map(ArraySizeExpr)

  def array[A <: Expr](elementExpr: P[A]): P[ArrayExpr[A]] =
    P(arrayPrefixWithCrlf ~ elementExpr.rep(sep = crlf)).map {
      case (size, values) =>
        require(size.n == values.size)
        ArrayExpr(values)
    }

  val bytesOptArrayElement: P[BytesOptExpr] = P(length ~ crlf ~ byteString.?).map {
    case (size, _) if size.n == -1 =>
      BytesOptExpr(None)
    case (size, value) =>
      BytesOptExpr(value.map(_.value))
  }

  val stringOptArrayElement: P[StringOptExpr] = P(length ~ crlf ~ string.?).map {
    case (size, _) if size.n == -1 =>
      StringOptExpr(None)
    case (size, value) =>
      StringOptExpr(value.map(_.v))
  }

  val integerArrayElement: P[NumberExpr] = P(number)

  val stringOptArrayWithCrLf: P[ArrayExpr[StringOptExpr]] = P(array(stringOptArrayElement) ~ crlf.?)
  val integerArrayWithCrLf: P[ArrayExpr[NumberExpr]]      = P(array(integerArrayElement) ~ crlf.?)

  val bulkBytesWithCrLf: P[BytesOptExpr]   = P((length ~ crlf).flatMap(l => bulkBytesRest(l.n)))
  val bulkStringWithCrLf: P[StringOptExpr] = P((length ~ crlf).flatMap(l => bulkStringRest(l.n)))

  def bulkBytesRest(l: Int): P[BytesOptExpr] = {
    if (l == -1) {
      P(End).!.map(_ => BytesOptExpr(None))
    } else {
      P(byteString ~ crlf).map(v => BytesOptExpr(Some(v.value)))
    }
  }

  def bulkStringRest(l: Int): P[StringOptExpr] = {
    if (l == -1) {
      P(End).!.map(_ => StringOptExpr(None))
    } else {
      P(string ~ crlf).map(v => StringOptExpr(Some(v.v)))
    }
  }

  val simpleStringReply: P[Expr]   = P(simpleWithCrLf | errorWithCrLf)
  val integerReply: P[Expr]        = P(integerWithCrLf | errorWithCrLf)
  val integerArrayReply: P[Expr]   = P(integerArrayWithCrLf | errorWithCrLf)
  val stringOptArrayReply: P[Expr] = P(stringOptArrayWithCrLf | errorWithCrLf)
  val bulkStringReply: P[Expr]     = P(bulkStringWithCrLf | errorWithCrLf)
  val bulkBytesReply: P[Expr]      = P(bulkBytesWithCrLf | errorWithCrLf)
}
