package com.github.j5ik2o.reactive.redis

import java.util.concurrent.atomic.AtomicReference

import akka.actor.{ ActorRef, ActorSystem }
import akka.stream.ActorMaterializer
import akka.testkit.{ ImplicitSender, TestKit }
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{ Seconds, Span }
import org.scalatest.{ BeforeAndAfterAll, FunSpecLike }

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.duration._

abstract class ActorSpec(_system: ActorSystem)
    extends TestKit(_system)
    with ImplicitSender
    with FunSpecLike
    with BeforeAndAfterAll
    with ScalaFutures {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(scaled(Span(3, Seconds)))

  implicit val materializer = ActorMaterializer()

  implicit val timeout = Timeout(15 seconds)

  val clientRef: AtomicReference[ActorRef] = new AtomicReference()

  var executor: Option[RedisAPIExecutor] = None

  def client: ActorRef = clientRef.get

  override protected def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }

}
