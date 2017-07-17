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

  case class ArraySizeExpr(value: Int) extends Expr

  case class ArrayExpr[A <: Expr](values: Seq[A] = Seq.empty) extends Expr

}

trait CommandResponseParserSupport extends RegexParsers {

  import CommandResponseParser._

  override def skipWhitespace: Boolean = false

  private lazy val CRLF = """\r\n""".r

  private lazy val ERROR: Parser[ErrorExpr] = elem('-') ~> """[a-zA-Z0-9. ]+""".r ^^ { msg =>
    ErrorExpr(msg)
  }

  private lazy val LENGTH: Parser[LengthExpr] = elem('$') ~> """[0-9-]+""".r ^^ { n =>
    LengthExpr(n.toInt)
  }

  private lazy val SIMPLE: Parser[SimpleExpr] = elem('+') ~> """[a-zA-Z0-9. ]+""".r ^^ { msg =>
    SimpleExpr(msg)
  }

  private lazy val NUMBER: Parser[NumberExpr] = elem(':') ~> """[-0-9]+""".r ^^ { n =>
    NumberExpr(n.toInt)
  }

  private lazy val STRING: Parser[String] = """.*[^\r\n]""".r

  private lazy val VALUE: Parser[StringExpr] = STRING ^^ { s =>
    StringExpr(s)
  }

  private lazy val ARRAY_PREFIX: Parser[Int] = elem('*') ~> """[0-9]+""".r ^^ (_.toInt)

  private lazy val errorWithCrLf: Parser[Expr] = ERROR <~ CRLF

  private lazy val simpleWithCrLf: Parser[Expr] = SIMPLE <~ CRLF

  private lazy val integerWithCrLf: Parser[NumberExpr] = NUMBER <~ CRLF

  private lazy val arrayPrefixWithCrLf: Parser[ArraySizeExpr] = ARRAY_PREFIX <~ CRLF ^^ { n =>
    ArraySizeExpr(n)
  }

  def arrayReply[A <: Expr](elementExpr: Parser[A]): Parser[ArrayExpr[A]] =
    arrayPrefixWithCrLf ~ repsep(
      elementExpr,
      CRLF
    ) ^^ {
      case size ~ values =>
        require(size.value == values.size)
        ArrayExpr(values)
    }

  private lazy val stringOptArrayElement: Parser[StringOptExpr] = LENGTH ~ CRLF ~ opt(STRING) ^^ {
    case size ~ _ ~ value =>
      // require(size.value == -1 || size.value == value.size)
      StringOptExpr(value)
  }

  private lazy val integerArrayElement: Parser[NumberExpr] = opt(LENGTH <~ CRLF) ~ NUMBER ^^ {
    case size ~ value =>
      // require(size.map(_.value).fold(true)(_ == value.size))
      value
  }

  private lazy val stringOptArrayWithCrLf: Parser[ArrayExpr[StringOptExpr]] = arrayReply(stringOptArrayElement)

  private lazy val integerArrayWithCrLf: Parser[ArrayExpr[NumberExpr]] = arrayReply(integerArrayElement)

  private lazy val bulkStringWithCrLf: Parser[StringOptExpr] = LENGTH ~ CRLF ~ opt(STRING <~ CRLF) ^^ {
    case l ~ _ ~ s =>
      StringOptExpr(s)
  }

  lazy val simpleStringReply: Parser[Expr] = simpleWithCrLf | errorWithCrLf

  lazy val integerReply: Parser[Expr] = integerWithCrLf | errorWithCrLf

  lazy val arrayPrefixWithCrLfOrErrorWithCrLf: Parser[Expr] = arrayPrefixWithCrLf | errorWithCrLf

  lazy val integerArrayReply: Parser[Expr] = integerArrayWithCrLf | errorWithCrLf

  lazy val stringOptArrayReply: Parser[Expr] = stringOptArrayWithCrLf | errorWithCrLf

  lazy val bulkStringReply: Parser[Expr] = bulkStringWithCrLf | errorWithCrLf

  protected val responseParser: Parser[Expr]

  def parseResponseToExprWithInput(in: Reader[Char]): (Expr, Input) = parse(responseParser, in) match {
    case Success(result, next) => (result, next)
    case Failure(msg, _)       => throw new Exception(msg)
    case Error(msg, _)         => throw new Exception(msg)
  }

}
