package com.gvolpe.streams

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object TaskApp extends App with TaskUtils {

  val f1: Future[List[Int]] = Future{ println("future 1"); List(1, 2, 3) }
  val f2: Future[List[Int]] = Future{ println("future 2"); List(4, 5, 6) }
  val f3: Future[List[Int]] = Future{ println("future 3"); throw new Exception("whatever")}

  val task1: Task[List[Int]] = futureToTask(f1)
  val task2: Task[List[Int]] = futureToTask(f2)
  val task3: Task[List[Int]] = futureToTask(f3)

  val results: Task[List[Int]] = for {
    v1 <- task1
    v2 <- task2
    v3 <- task3
  } yield v1 ::: v2 ::: v3

  println(List(1, 2).right)
  println(results.attemptRun)

  val n1 = Task.now(4)
  val n2 = Task.now(7)

  val sum: Task[Int] = for {
    v1 <- n1
    v2 <- n2
  } yield v1 + v2

  println(sum.attemptRun)

}

trait TaskUtils {

  def futureToTask[A](f: Future[A]): Task[A] = {
    Task async { cb =>
      f onComplete {
        case Success(v) => cb(\/.right(v))
        case Failure(t) => cb(\/.left(t))
      }
    }
  }

}
