package com.github.j5ik2o.reactive.redis

import cats._

import scala.annotation.tailrec

sealed trait Result[+A] {
  def isSuspended: Boolean
  def value: A
}

object Result {

  implicit val functor: Functor[Result] = new Functor[Result] {
    override def map[A, B](fa: Result[A])(f: A => B): Result[B] = fa match {
      case Suspended      => Suspended
      case p: Provided[A] => Provided(f(p.value))
    }
  }

  implicit val foldable: Foldable[Result] = new Foldable[Result] {
    override def foldLeft[A, B](fa: Result[A], b: B)(f: (B, A) => B): B = fa match {
      case Suspended      => b
      case p: Provided[A] => f(b, p.value)
    }

    override def foldRight[A, B](fa: Result[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = fa match {
      case Suspended      => lb
      case p: Provided[A] => f(p.value, lb)
    }
  }

  implicit val monad: Monad[Result] = new Monad[Result] {

    override def flatMap[A, B](fa: Result[A])(f: A => Result[B]): Result[B] = {
      fa match {
        case Suspended      => Suspended
        case p: Provided[A] => f(p.value)
      }
    }

    @tailrec
    override def tailRecM[A, B](a: A)(f: A => Result[Either[A, B]]): Result[B] = f(a) match {
      case Suspended          => Suspended
      case Provided(Left(a1)) => tailRecM(a1)(f)
      case Provided(Right(b)) => Provided(b)
    }

    override def pure[A](x: A): Result[A] = Provided[A](x)

  }

}

case object Suspended extends Result[Nothing] {
  override def isSuspended: Boolean = true

  override def value: Nothing = throw new NoSuchElementException("The value has Suspended is Nothing")
}

case class Provided[A](value: A) extends Result[A] {
  override def isSuspended: Boolean = false
}
