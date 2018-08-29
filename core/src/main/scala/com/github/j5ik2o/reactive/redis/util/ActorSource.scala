package com.github.j5ik2o.reactive.redis.util

import akka.actor.ActorRef
import akka.stream.scaladsl.Source

import scala.concurrent.Future

object ActorSource {

  def apply[E](bufferSize: Int): Source[E, Future[ActorRef]] = Source.fromGraph(new ActorSourceStage[E](bufferSize))

  def create[E](bufferSize: Int): Source[Nothing, Future[ActorRef]] = apply(bufferSize)

}
