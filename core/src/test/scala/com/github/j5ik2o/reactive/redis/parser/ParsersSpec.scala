package com.github.j5ik2o.reactive.redis.parser

import fastparse.core.Parsed
import org.scalatest.FreeSpec

class ParsersSpec extends FreeSpec {
  "ParsersSpec" - {
    "integerArrayReply" in {
      Parsers.integerArrayReply.parse("*1\r\n:1\r\n") match {
        case Parsed.Success(_, _) =>
        case Parsed.Failure(_, _, _) =>
          fail()
      }
    }
    "bulkStringWithCrLf" in {
      Parsers.bulkStringWithCrLf.parse("$-1\r\n") match {
        case Parsed.Success(v, _) =>
        case Parsed.Failure(_, _, _) =>
          fail()
      }
    }
  }
}
