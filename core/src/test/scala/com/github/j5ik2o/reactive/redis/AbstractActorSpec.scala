package com.github.j5ik2o.reactive.redis

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import cats.data.NonEmptyList
import monix.eval.Task
import monix.execution.Scheduler
import org.scalactic.anyvals.PosInt
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.prop.PropertyChecks
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

abstract class AbstractActorSpec(_system: ActorSystem)
    extends TestKit(_system)
    with FreeSpecLike
    with Matchers
    with BeforeAndAfterAll
    with TimeFactorSupport
    with ScalaFutures
    with PropertyChecks
    with RedisSpecSupport
    with ScalaCheckSupport {

  implicit val scheduler: Scheduler = Scheduler(system.dispatcher)

  val logger = LoggerFactory.getLogger(getClass)

  override def waitFor(): Unit = Thread.sleep((500 * timeFactor milliseconds).toMillis)

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(60 * timeFactor seconds, 1 * timeFactor seconds)

  override implicit val generatorDrivenConfig = PropertyCheckConfiguration(minSuccessful = PosInt(5))

  implicit val materializer: ActorMaterializer = ActorMaterializer()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    shutdown()
  }

  protected def createConnectionPool(peerConfigs: NonEmptyList[PeerConfig]): RedisConnectionPool[Task]

}
