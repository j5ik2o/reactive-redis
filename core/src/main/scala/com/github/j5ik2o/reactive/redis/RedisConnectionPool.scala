package com.github.j5ik2o.reactive.redis
import akka.actor.{ ActorSystem, PoisonPill }
import akka.pattern.ask
import akka.routing._
import akka.util.Timeout
import cats.data.ReaderT
import cats.{ Monad, MonadError }
import com.github.j5ik2o.reactive.redis.pool.RedisConnectionActor.{ BorrowConnection, ConnectionGotten }
import com.github.j5ik2o.reactive.redis.pool.{ RedisConnectionActor, RedisConnectionPoolActor }
import monix.eval.Task
import monix.execution.Scheduler

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

  def ofRoundRobin(
      sizePerPeer: Int,
      peerConfigs: Seq[PeerConfig],
      newConnection: PeerConfig => RedisConnection,
      resizer: Option[Resizer] = None,
      passingTimeout: FiniteDuration = 3 seconds
  )(implicit system: ActorSystem, scheduler: Scheduler, ME: MonadError[Task, Throwable]): RedisConnectionPool[Task] =
    apply(RoundRobinPool(sizePerPeer, resizer), peerConfigs, newConnection, passingTimeout)(
      system,
      scheduler,
      ME
    )

  def ofBalancing(sizePerPeer: Int,
                  peerConfigs: Seq[PeerConfig],
                  newConnection: PeerConfig => RedisConnection,
                  passingTimeout: FiniteDuration = 3 seconds)(
      implicit system: ActorSystem,
      scheduler: Scheduler,
      ME: MonadError[Task, Throwable]
  ): RedisConnectionPool[Task] =
    apply(BalancingPool(sizePerPeer), peerConfigs, newConnection, passingTimeout)(
      system,
      scheduler,
      ME
    )

  def apply(pool: Pool,
            peerConfigs: Seq[PeerConfig],
            newConnection: PeerConfig => RedisConnection,
            passingTimeout: FiniteDuration = 3 seconds)(
      implicit system: ActorSystem,
      scheduler: Scheduler,
      ME: MonadError[Task, Throwable]
  ): RedisConnectionPool[Task] =
    new DefaultPool(pool, peerConfigs, newConnection, passingTimeout)(system, scheduler, ME)

}

abstract class RedisConnectionPool[M[_]](implicit ME: MonadError[M, Throwable]) {

  def withConnectionF[T](f: RedisConnection => M[T]): M[T] = withConnectionM(ReaderT(f))

  def withConnectionM[T](reader: ReaderRedisConnection[M, T]): M[T]

  def borrowConnection: M[RedisConnection]

  def returnConnection(redisConnection: RedisConnection): M[Unit]

  def numActive: Int

  def clear(): Unit

  def dispose(): Unit

}

private class DefaultPool(
    pool: Pool,
    peerConfigs: Seq[PeerConfig],
    newConnection: PeerConfig => RedisConnection,
    passingTimeout: FiniteDuration = 3 seconds
)(implicit system: ActorSystem, scheduler: Scheduler, ME: MonadError[Task, Throwable])
    extends RedisConnectionPool[Task]() {

  private val connectionPoolActor =
    system.actorOf(
      RedisConnectionPoolActor.props(
        pool,
        peerConfigs.map(v => RedisConnectionActor.props(v, passingTimeout, newConnection))
      )
    )

  implicit val to: Timeout = passingTimeout

  override def withConnectionM[T](reader: ReaderRedisConnection[Task, T]): Task[T] = {
    for {
      con    <- borrowConnection
      result <- reader.run(con)
      _      <- returnConnection(con)
    } yield result
  }

  override def borrowConnection: Task[RedisConnection] = Task.deferFutureAction { implicit ec =>
    (connectionPoolActor ? BorrowConnection).mapTo[ConnectionGotten].map(_.redisConnection)(ec)
  }

  override def returnConnection(redisConnection: RedisConnection): Task[Unit] = Task.pure(())

  override def numActive: Int = pool.nrOfInstances(system) * peerConfigs.size

  override def clear(): Unit = {}

  override def dispose(): Unit = { connectionPoolActor ! PoisonPill }
}
