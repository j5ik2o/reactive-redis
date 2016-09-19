package com.github.j5ik2o.reactive.redis.connection

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.github.j5ik2o.reactive.redis._
import com.github.j5ik2o.reactive.redis.connection.ConnectionProtocol.{ QuitRequest, SelectRequest }

import scala.reflect.ClassTag

trait ConnectionStreamAPI extends BaseStreamAPI {

  // --- AUTH

  // --- ECHO

  // --- PING

  // --- QUIT
  val quit = Source.single(QuitRequest)

  // --- SELECT
  def select(index: Int)(implicit c: ClassTag[SelectRequest]): Source[SelectRequest, NotUsed] = Source.single(SelectRequest(index))

}
