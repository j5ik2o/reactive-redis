package com.github.j5ik2o.reactive.redis.parser

import com.github.j5ik2o.reactive.redis.parser.model._

@SuppressWarnings(
  Array("org.wartremover.warts.Product",
        "org.wartremover.warts.Serializable",
        "org.wartremover.warts.Equals",
        "org.wartremover.warts.EitherProjectionPartial")
)
object StringParsers {
  import fastparse.all._
  val QUEUED         = "QUEUED"
  val OK             = "OK"
  val digit: P0      = P(CharIn('0' to '9'))
  val lowerAlpha: P0 = P(CharIn('a' to 'z'))
  val upperAlpha: P0 = P(CharIn('A' to 'Z'))
  val alpha: P0      = P(lowerAlpha | upperAlpha)
  val alphaDigit: P0 = P(alpha | digit)

  val crlf: P0 = P("\r\n")

  val error: P[ErrorExpr] = P("-" ~ (!crlf ~/ AnyChar).rep(1).!).map(ErrorExpr)
  val length: P[LengthExpr] = P("$" ~ "-".!.? ~ digit.rep(1).!).map {
    case (m, n) =>
      LengthExpr(m.map(_ => -1).getOrElse(1) * n.toInt)
  }
  val simple: P[SimpleExpr] = P("+" ~ alphaDigit.rep(1).!).map(SimpleExpr)
  val number: P[NumberExpr] = P(":" ~ "-".!.? ~ alphaDigit.rep(1).!).map {
    case (minus, n) =>
      NumberExpr(minus.map(_ => -1).getOrElse(1) * n.toInt)
  }
  val string: P[StringExpr] = P((!crlf ~/ AnyChar).rep.!).map(StringExpr)
  val arrayPrefix: P[Int]   = P("*" ~ digit.rep(1).!).map(_.toInt)

  val errorWithCrLf: P[ErrorExpr]           = P(error ~ crlf)
  val simpleWithCrLf: P[SimpleExpr]         = P(simple ~ crlf)
  val integerWithCrLf: P[NumberExpr]        = P(number ~ crlf)
  val arrayPrefixWithCrLf: P[ArraySizeExpr] = P(arrayPrefix ~ crlf).map(ArraySizeExpr)

  def array[A <: Expr](elementExpr: P[A]): P[ArrayExpr[A]] =
    P(arrayPrefixWithCrLf ~ elementExpr.rep(sep = crlf)).map {
      case (size, values) =>
        require(size.value == values.size)
        ArrayExpr(values)
    }

  val stringOptArrayElement: P[StringOptExpr] = P(length ~ crlf ~ string.?).map {
    case (size, _) if size.value == -1 =>
      StringOptExpr(None)
    case (size, value) =>
      StringOptExpr(value.map(_.value))
  }

  val integerArrayElement: P[NumberExpr] = P(number)

  val stringOptArrayWithCrLf: P[ArrayExpr[StringOptExpr]] = P(array(stringOptArrayElement) ~ crlf.?)
  val integerArrayWithCrLf: P[ArrayExpr[NumberExpr]]      = P(array(integerArrayElement) ~ crlf.?)
  def bulkStringRest(l: Int): P[StringOptExpr] = {
    if (l == -1) {
      P(End).!.map(_ => StringOptExpr(None))
    } else {
      P(string ~ crlf).map(v => StringOptExpr(Some(v.value)))
    }
  }

  val arrayPrefixWithCrLfOrErrorWithCrLf: P[Expr] = P(arrayPrefixWithCrLf | errorWithCrLf)

  private val bulkStringWithCrLf: P[StringOptExpr] = P((length ~ crlf).flatMap(l => bulkStringRest(l.value)))

  val simpleStringReply: P[Expr] = P(simpleWithCrLf | errorWithCrLf)
  val integerReply: P[Expr]      = P(integerWithCrLf | errorWithCrLf)
  val integerArrayReply: P[Expr] = P(integerArrayWithCrLf | errorWithCrLf)

  val stringOptArrayReply: P[Expr] = P(stringOptArrayWithCrLf | errorWithCrLf)

  val stringArrayReply: P[Expr] = P(stringOptArrayWithCrLf.map { v =>
    ArrayExpr(v.values.map(_.toStringExpr))
  } | errorWithCrLf)

  val bulkStringReply: P[Expr] = P(bulkStringWithCrLf | errorWithCrLf)
}
