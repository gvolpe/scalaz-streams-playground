package com.gvolpe.streams

import scala.language.higherKinds
import scala.util.Try
import scalaz._
import scalaz.concurrent.Task
import Scalaz._
import stream._

object Streams2 extends App {

  type ProcessT[A] = Process[Task, A]

  val input = Seq("1", "2", "FOO", "3")

  val source: ProcessT[String] = Process.emitAll(Seq("1", "2", "FOO", "3"))

  val output: Seq[stream.Writer[Task, String, Int]] = input map sum

  def sum(str: String) = {
    parseNumber(str)
    .toRightDisjunction("It's not a number!")
    .point[ProcessT]
    .mapO(_ + 1)
  }

  println(output)

  def show(str: String) = Task.delay(println(str))
  def consoleSink: Sink[Task, Int] = Process.constant { n => show(n.toString) }

//  output foreach { p =>
//    p.mapO(_ * 2)
//      .mapW("ERROR: ".concat(_))
//      .observeO(consoleSink)
//      .observeW(io.stdOutLines)
//      .run.run
//  }

  output foreach { p =>
    p.mapO(n => "VALUE: ".concat((n * 2).toString))
      .mapW("ERROR: ".concat)
      .observeO(io.stdOutLines)
      .observeW(io.stdOutLines)
      .run.run
  }

  def parseNumber(str: String): Option[Int] = Try(str.toInt).toOption

  val intParser: Channel[Task, String, String \/ Int] = Process.constant { (s: String) =>
    Task.delay {
      import scalaz.syntax.std.string._
      s.parseInt.disjunction.leftMap(_ => s"Failed parsing: $s")
    }
  }

  val parsed = Process.emitAll(input) through intParser

  //parsed.observeW(Process.eval(Task.delay(println)))
  //println(parsed.runLog.run)

  val writer: stream.Writer[Task, String, Int] = Process.emitAll(input).toSource through intParser

  val result = writer
    .mapO(_ * 2)
    .observeW(io.stdOutLines)
    .runLog.run

  print2ln(result)

  // Stream Stats using a Monoid

  case class StreamStats(successes: Int, failures: Int)

  implicit val statsMonoid = Monoid.instance[StreamStats] ({ (a, b) =>
    StreamStats(a.successes + b.successes, a.failures + b.failures)
  }, StreamStats(0, 0))

  def reportStats[A, B](f: StreamStats => Task[Unit]): Process1[A \/ B, A \/ B] = {
    import scalaz.syntax.monoid._
    def go(acc: StreamStats): Process1[A \/ B, A \/ B] = {
      // Receives one input or executes the final sum
      Process.receive1Or[A \/ B, A \/ B](
        f(acc).attemptRun.fold(Process.fail, _ => Process.halt)
      ) { rx => {
          val sum = go(acc |+| rx.fold(_ => StreamStats(0, 1),
                                       _ => StreamStats(1, 0)))
          sum ++ Process.emit(rx)
        }
      }
    }

    go(mzero[StreamStats])
  }

  val stats = parsed pipe reportStats(s => Task.delay(print2ln(s)))
  stats.runLog.run

  def print2ln[A](a: A): Unit = {
    println; println(a)
  }

}
