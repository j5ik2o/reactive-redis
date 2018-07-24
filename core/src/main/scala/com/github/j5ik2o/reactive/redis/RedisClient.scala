package com.github.j5ik2o.reactive.redis

import java.util.UUID

import akka.actor.ActorSystem
import cats.data.{ Reader, ReaderT }
import cats.implicits._
import com.github.j5ik2o.reactive.redis.command._
import com.github.j5ik2o.reactive.redis.feature._
import monix.execution.Scheduler

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object RedisClient {

  def apply()(implicit system: ActorSystem): RedisClient = new RedisClient()

}

class RedisClient(implicit system: ActorSystem)
    extends StringsFeature
    with ListsFeature
    with HashesFeature
    with SetsFeature
    with SortedSetsFeature
    with GeoFeature
    with PubSubFeature
    with HyperLogLogFeature
    with StreamsFeature
    with KeysFeature
    with ConnectionFeature
    with ServerFeature
    with ClusterFeature
    with TransactionsFeature {

  def send[C <: CommandRequestBase](cmd: C): ReaderTTaskRedisConnection[cmd.Response] = ReaderT(_.send(cmd))

  def validate(timeout: Duration)(implicit scheduler: Scheduler): Reader[RedisConnection, Boolean] = Reader {
    connection =>
      val id = UUID.randomUUID().toString
      Await.result(ping(Some(id)).run(connection).runAsync, timeout).exists(_ === id)
  }
}
