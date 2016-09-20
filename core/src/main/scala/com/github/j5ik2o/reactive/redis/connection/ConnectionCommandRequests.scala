package com.github.j5ik2o.reactive.redis.connection

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.github.j5ik2o.reactive.redis._
import com.github.j5ik2o.reactive.redis.connection.ConnectionProtocol.{ QuitRequest, SelectRequest }

import scala.reflect.ClassTag

trait ConnectionCommandRequests {

  // --- AUTH

  // --- ECHO

  // --- PING

  // --- QUIT
  val quitRequest: Source[QuitRequest.type, NotUsed] = Source.single(QuitRequest)

  // --- SELECT
  def selectRequest(index: Int): Source[SelectRequest, NotUsed] = Source.single(SelectRequest(index))

}
