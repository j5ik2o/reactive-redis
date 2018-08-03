package com.github.j5ik2o.reactive.redis.parser

import java.util.concurrent.TimeUnit

import StringParsers._
import com.github.j5ik2o.reactive.redis.parser.util.instance.Reference
import org.openjdk.jmh.annotations._
import fastparse.all._

@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
class Parsers {

  @Benchmark
  def fastparse(): Unit = {
    val Parsed.Success(result, _) = P(errorReply | simpleStringReply | bulkStringReply).parse("$1\r\nA\r\n")
    ()
  }

  @Benchmark
  def defaults(): Unit = {
    val expr = CustomParsers.getParer(Reference)
    Reference.run(expr)("$1\r\nA\r\n").right.get
    ()
  }

}
