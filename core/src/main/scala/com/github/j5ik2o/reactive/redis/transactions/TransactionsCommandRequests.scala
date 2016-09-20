package com.github.j5ik2o.reactive.redis.transactions

import akka.NotUsed
import akka.stream.scaladsl.Source

trait TransactionsCommandRequests {

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
