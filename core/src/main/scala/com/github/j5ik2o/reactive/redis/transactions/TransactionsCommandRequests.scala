package com.github.j5ik2o.reactive.redis.transactions

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.github.j5ik2o.reactive.redis.transactions.TransactionsProtocol._

trait TransactionsCommandRequests {

  // --- DISCARD
  val discardRequest: Source[DiscardRequest, NotUsed] = Source.single(DiscardRequest())

  // --- EXEC
  val execRequest: Source[ExecRequest, NotUsed] = Source.single(ExecRequest())

  // --- MULTI
  val multiRequest: Source[MultiRequest, NotUsed] = Source.single(MultiRequest())

  // --- UNWATCH
  val unwatchRequest: Source[UnWatchRequest, NotUsed] = Source.single(UnWatchRequest())

  // --- WATCH
  val watchRequest: Source[WatchRequest, NotUsed] = Source.single(WatchRequest())

}
