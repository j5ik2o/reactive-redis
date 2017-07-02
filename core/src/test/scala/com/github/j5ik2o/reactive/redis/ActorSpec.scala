package com.github.j5ik2o.reactive.redis

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, _}

abstract class ActorSpec(_system: ActorSystem)
    extends TestKit(_system)
    with ImplicitSender
    with FunSpecLike
    with BeforeAndAfterAll
    with ScalaFutures {

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(1, Seconds)))

  implicit val materializer = ActorMaterializer()

  implicit val timeout = Timeout(15 seconds)

  override protected def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
    super.beforeAll()
  }

}
