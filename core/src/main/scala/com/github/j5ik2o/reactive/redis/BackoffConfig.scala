package com.github.j5ik2o.reactive.redis

import scala.concurrent.duration._

final class BackoffConfig(
    val minBackoff: FiniteDuration,
    val maxBackoff: FiniteDuration,
    val randomFactor: Double,
    val maxRestarts: Int
) {

  override def equals(other: Any): Boolean = other match {
    case that: BackoffConfig =>
      minBackoff == that.minBackoff &&
      maxBackoff == that.maxBackoff &&
      randomFactor == that.randomFactor &&
      maxRestarts == that.maxRestarts
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(minBackoff, maxBackoff, randomFactor, maxRestarts)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"BackoffConfig($minBackoff, $maxBackoff, $randomFactor, $maxRestarts)"

}

object BackoffConfig {

  def apply(
      minBackoff: FiniteDuration = 3 seconds,
      maxBackoff: FiniteDuration = 30 seconds,
      randomFactor: Double = 0.2,
      maxRestarts: Int = -1
  ): BackoffConfig =
    new BackoffConfig(minBackoff, maxBackoff, randomFactor, maxRestarts)

  def unapply(self: BackoffConfig): Option[(FiniteDuration, FiniteDuration, Double, Int)] =
    Some((self.minBackoff, self.maxBackoff, self.randomFactor, self.maxRestarts))

  def create(
      minBackoff: FiniteDuration = 3 seconds,
      maxBackoff: FiniteDuration = 30 seconds,
      randomFactor: Double = 0.2,
      maxRestarts: Int = -1
  ): BackoffConfig =
    apply(minBackoff, maxBackoff, randomFactor, maxRestarts)

}
