package com.github.j5ik2o.reactive.redis
import akka.actor.{ ActorSystem, PoisonPill }
import akka.pattern.ask
import akka.routing._
import akka.stream.Supervision
import akka.util.Timeout
import cats.data.ReaderT
import cats.{ Monad, MonadError }
import com.github.j5ik2o.reactive.redis.pool.RedisConnectionActor.{ BorrowConnection, ConnectionGotten }
import com.github.j5ik2o.reactive.redis.pool.{ RedisConnectionActor, RedisConnectionPoolActor }
import monix.eval.Task
import monix.execution.Scheduler
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

object RedisConnectionPool {

  implicit val taskMonadError: MonadError[Task, Throwable] = new MonadError[Task, Throwable] {
    private val taskMonad = implicitly[Monad[Task]]

    override def pure[A](x: A): Task[A] = taskMonad.pure(x)

    override def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B] = taskMonad.flatMap(fa)(f)

    override def tailRecM[A, B](a: A)(f: A => Task[Either[A, B]]): Task[B] = taskMonad.tailRecM(a)(f)

    override def raiseError[A](e: Throwable): Task[A] = Task.raiseError(e)

    override def handleErrorWith[A](fa: Task[A])(f: Throwable => Task[A]): Task[A] = fa.onErrorRecoverWith {
      case t: Throwable => f(t)
    }
  }

  def ofSingleConnection(redisConnection: RedisConnection)(implicit system: ActorSystem): RedisConnectionPool[Task] =
    new SinglePool(redisConnection)

  def ofRoundRobin(
      sizePerPeer: Int,
      peerConfigs: Seq[PeerConfig],
      newConnection: (PeerConfig, Option[Supervision.Decider]) => RedisConnection,
      resizer: Option[Resizer] = None,
      supervisionDecider: Option[Supervision.Decider] = None,
      passingTimeout: FiniteDuration = 5 seconds
  )(implicit system: ActorSystem, scheduler: Scheduler, ME: MonadError[Task, Throwable]): RedisConnectionPool[Task] =
    apply(RoundRobinPool(sizePerPeer, resizer), peerConfigs, newConnection, supervisionDecider, passingTimeout)(
      system,
      scheduler
    )

  def ofBalancing(sizePerPeer: Int,
                  peerConfigs: Seq[PeerConfig],
                  newConnection: (PeerConfig, Option[Supervision.Decider]) => RedisConnection,
                  supervisionDecider: Option[Supervision.Decider] = None,
                  passingTimeout: FiniteDuration = 5 seconds)(
      implicit system: ActorSystem,
      scheduler: Scheduler,
      ME: MonadError[Task, Throwable]
  ): RedisConnectionPool[Task] =
    apply(BalancingPool(sizePerPeer), peerConfigs, newConnection, supervisionDecider, passingTimeout)(
      system,
      scheduler
    )

  def apply(pool: Pool,
            peerConfigs: Seq[PeerConfig],
            newConnection: (PeerConfig, Option[Supervision.Decider]) => RedisConnection,
            supervisionDecider: Option[Supervision.Decider] = None,
            passingTimeout: FiniteDuration = 3 seconds)(
      implicit system: ActorSystem,
      scheduler: Scheduler
  ): RedisConnectionPool[Task] =
    new AkkaPool(pool, peerConfigs, newConnection, supervisionDecider, passingTimeout)(system, scheduler)

  private class AkkaPool(
      pool: Pool,
      val peerConfigs: Seq[PeerConfig],
      newConnection: (PeerConfig, Option[Supervision.Decider]) => RedisConnection,
      supervisionDecider: Option[Supervision.Decider] = None,
      passingTimeout: FiniteDuration = 3 seconds
  )(implicit system: ActorSystem, scheduler: Scheduler)
      extends RedisConnectionPool[Task]() {

    private val connectionPoolActor =
      system.actorOf(
        RedisConnectionPoolActor.props(
          pool,
          peerConfigs.map(v => RedisConnectionActor.props(v, newConnection, supervisionDecider, passingTimeout))
        )
      )

    private implicit val to: Timeout = passingTimeout

    override def withConnectionM[T](reader: ReaderRedisConnection[Task, T]): Task[T] = {
      borrowConnection.flatMap { con =>
        reader.run(con).doOnFinish { _ =>
          returnConnection(con)
        }
      }
    }

    override def borrowConnection: Task[RedisConnection] = Task.deferFutureAction { implicit ec =>
      (connectionPoolActor ? BorrowConnection).mapTo[ConnectionGotten].map(_.redisConnection)(ec)
    }

    override def returnConnection(redisConnection: RedisConnection): Task[Unit] = {
      Task.pure(())
    }

    override def numActive: Int = pool.nrOfInstances(system) * peerConfigs.size

    override def clear(): Unit = {}

    override def dispose(): Unit = { connectionPoolActor ! PoisonPill }
  }

  private class SinglePool(redisConnection: RedisConnection)(implicit system: ActorSystem)
      extends RedisConnectionPool[Task]() {

    override def peerConfigs: Seq[PeerConfig] = Seq(redisConnection.peerConfig)

    override def withConnectionM[T](reader: ReaderRedisConnection[Task, T]): Task[T] = reader(redisConnection)

    override def borrowConnection: Task[RedisConnection] = Task.pure(redisConnection)

    override def returnConnection(redisConnection: RedisConnection): Task[Unit] = Task.pure(())

    def invalidateConnection(redisConnection: RedisConnection): Task[Unit] = Task.pure(())

    override def numActive: Int = 1

    override def clear(): Unit = {}

    override def dispose(): Unit = redisConnection.shutdown()

  }

}

abstract class RedisConnectionPool[M[_]] {

  def peerConfigs: Seq[PeerConfig]

  protected val logger = LoggerFactory.getLogger(getClass)

  def withConnectionF[T](f: RedisConnection => M[T]): M[T] = withConnectionM(ReaderT(f))

  def withConnectionM[T](reader: ReaderRedisConnection[M, T]): M[T]

  def borrowConnection: M[RedisConnection]

  def returnConnection(redisConnection: RedisConnection): M[Unit]

  def numActive: Int

  def clear(): Unit

  def dispose(): Unit

}
