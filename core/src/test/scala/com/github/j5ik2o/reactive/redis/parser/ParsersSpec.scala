package com.github.j5ik2o.reactive.redis.parser

import java.nio.charset.{ Charset, StandardCharsets }

import com.github.j5ik2o.reactive.redis.parser.model.Expr
import com.github.j5ik2o.reactive.redis.parser.util.instance.{ Reference, Sliceable }
import fastparse.core.Parsed
import org.scalatest.FreeSpec
import scodec.bits.ByteVector
import com.github.j5ik2o.reactive.redis.parser.util.instance.ReferenceTypes.Parser

class ParsersSpec extends FreeSpec {
  val P = Reference
  def printResult[E](e: Either[E, Expr]) =
    e.fold(println, println)

  implicit val enc: Charset = StandardCharsets.UTF_8
  "ParsersSpec" - {
    "error" in {
      val expr = CustomParsers.getParer(P)
      printResult { P.run(expr)("-test\r\n") }
    }
    "crlf" in {
      val Parsed.Success(result, _) = ByteParsers.crlf.parse(ByteVector.encodeString("\r\n").right.get)
      println(result)
    }
    "digit" in {
      val Parsed.Success(result, _) = ByteParsers.digit.parse(ByteVector.encodeString("0").right.get)
      println(result)
    }
    "lowerAlpha" in {
      val Parsed.Success(result, _) = ByteParsers.lowerAlpha.parse(ByteVector.encodeString("a").right.get)
      println(result)
    }
    "integerArrayReply" in {
      ByteParsers.integerArrayReply.parse(ByteVector.encodeString("*1\r\n:1\r\n").right.get) match {
        case Parsed.Success(_, _) =>
        case f @ Parsed.Failure(_, _, _) =>
          fail(f.msg)
      }
    }
    "bulkStringWithCrLf" in {
      ByteParsers.bulkStringWithCrLf.parse(ByteVector.encodeString("$-1\r\n").right.get) match {
        case Parsed.Success(v, _) =>
        case f @ Parsed.Failure(_, _, _) =>
          fail(f.msg)
      }
    }
  }
}
