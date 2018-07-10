package com.github.j5ik2o.reactive.redis

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

abstract class ActorSpec(_system: ActorSystem)
    extends TestKit(_system)
    with FreeSpecLike
    with Matchers
    with BeforeAndAfterAll
    with TimeFactorSupport
    with ScalaFutures {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(15 * timeFactor seconds)

  implicit val materializer = ActorMaterializer()

  implicit val timeout = Timeout(15 seconds)

  override protected def afterAll(): Unit = {
    shutdown()
    super.beforeAll()
  }

}
