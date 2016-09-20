package com.github.j5ik2o.reactive.redis.transactions

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.{ BaseStreamAPI, RedisIOException }

import scala.concurrent.{ ExecutionContext, Future }

trait TransactionsStreamAPI extends BaseStreamAPI {
  import com.github.j5ik2o.reactive.redis.ResponseRegexs._
  // --- DISCARD
  val discardSource: Source[String, NotUsed] = Source.single("DISCARD")

  // --- EXEC
  val execSource: Source[String, NotUsed] = Source.single("EXEC")

  // --- MULTI
  def multiSource: Source[String, NotUsed] = Source.single("MULTI")

  // --- UNWATCH
  val unwatchSource: Source[String, NotUsed] = Source.single("UNWATCH")

  // --- WATCH
  val watchSource: Source[String, NotUsed] = Source.single("WATCH")

}
