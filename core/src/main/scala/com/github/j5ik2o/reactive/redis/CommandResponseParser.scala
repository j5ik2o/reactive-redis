package com.github.j5ik2o.reactive.redis

import scala.util.parsing.combinator.RegexParsers
import scala.util.parsing.input.Reader

object CommandResponseParser {

  sealed trait Expr

  trait HasSize extends Expr {
    val size: Int
  }

  case class ErrorExpr(msg: String) extends Expr

  case class LengthExpr(value: Int) extends Expr

  case class SimpleExpr(msg: String) extends Expr with HasSize {
    override val size: Int = msg.length
  }

  case class NumberExpr(value: Int) extends Expr with HasSize {
    override val size: Int = value.toString.length
  }

  case class StringExpr(value: String) extends Expr with HasSize {
    override val size: Int = value.length
  }

  case class StringOptExpr(value: Option[String]) extends Expr with HasSize {
    override val size: Int = value.fold(0)(_.length)
  }

  case class ArrayExpr[A <: Expr](values: Seq[A] = Seq.empty) extends Expr

}

abstract class CommandResponseParser[RT] extends RegexParsers {

  import CommandResponseParser._

  override def skipWhitespace: Boolean = false

  private lazy val CRLF = """\r\n""".r

  private lazy val ERROR: Parser[ErrorExpr] = elem('-') ~> """[a-zA-Z0-9. ]+""".r ^^ { msg => ErrorExpr(msg) }

  private lazy val LENGTH: Parser[LengthExpr] = elem('$') ~> """[0-9-]+""".r ^^ { n => LengthExpr(n.toInt) }

  private lazy val SIMPLE: Parser[SimpleExpr] = elem('+') ~> """[a-zA-Z0-9. ]+""".r ^^ { msg => SimpleExpr(msg) }

  private lazy val NUMBER: Parser[NumberExpr] = elem(':') ~> """[0-9]+""".r ^^ { n => NumberExpr(n.toInt) }

  private lazy val STRING: Parser[String] = """.*[^\r\n]""".r

  private lazy val VALUE: Parser[StringExpr] = STRING ^^ { s => StringExpr(s) }

  private lazy val ARRAY_PREFIX: Parser[Int] = elem('*') ~> """[0-9]+""".r ^^ {
    _.toInt
  }

  private lazy val errorWithCrLf: Parser[Expr] = ERROR <~ CRLF

  private lazy val simpleWithCrLf: Parser[Expr] = SIMPLE <~ CRLF

  private lazy val numberWithCrLf = NUMBER <~ CRLF

  lazy val simpleWithCrLfOrErrorWithCrLf: Parser[Expr] = simpleWithCrLf | errorWithCrLf

  lazy val numberWithCrLfOrErrorWithCrLf: Parser[Expr] = numberWithCrLf | errorWithCrLf

  private lazy val arrayPrefixWithCrLf: Parser[Int] = ARRAY_PREFIX <~ CRLF

  private lazy val stringArrayElement: Parser[StringExpr] = LENGTH ~ CRLF ~ VALUE ^^ {
    case size ~ _ ~ value =>
      require(size.value == value.size)
      value
  }

  private lazy val stringArrayWithCrLf: Parser[ArrayExpr[StringExpr]] = arrayPrefixWithCrLf ~ repsep(stringArrayElement, CRLF) ^^ {
    case size ~ values =>
      require(size == values.size)
      ArrayExpr(values)
  }

  private lazy val numberArrayElement: Parser[NumberExpr] = LENGTH ~ CRLF ~ NUMBER ^^ {
    case size ~ _ ~ value =>
      require(size.value == value.size)
      value
  }

  private lazy val numberArrayWithCrLf: Parser[ArrayExpr[NumberExpr]] = arrayPrefixWithCrLf ~ repsep(numberArrayElement, CRLF) ^^ {
    case size ~ values =>
      require(size == values.size)
      ArrayExpr(values)
  }

  lazy val stringArrayWithCrLfOrErrorWithCrLf: Parser[Expr] = stringArrayWithCrLf | errorWithCrLf

  private lazy val bulkStringWithCrLf: Parser[StringOptExpr] = LENGTH ~ CRLF ~ opt(STRING <~ CRLF) ^^ {
    case l ~ _ ~ s =>
      StringOptExpr(s)
  }

  lazy val bulkStringWithCrLfOrErrorWithCrLf: Parser[Expr] = bulkStringWithCrLf | errorWithCrLf

  private lazy val integerReplyWithCrLf = NUMBER <~ CRLF

  lazy val integerReplyWithCrLfOrErrorWithCrLf = integerReplyWithCrLf | errorWithCrLf

  protected val responseParser: Parser[RT]

  def parseResponse(in: Reader[Char]) = parse(responseParser, in) match {
    case Success(result, next) => (result, next)
    case Failure(msg, _)       => throw new Exception(msg)
    case Error(msg, _)         => throw new Exception(msg)
  }

}
