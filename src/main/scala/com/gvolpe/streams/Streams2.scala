package com.gvolpe.streams

import scala.language.higherKinds
import scala.util.Try
import scalaz._
import scalaz.concurrent.Task
import Scalaz._
import stream.{ Writer => StreamWriter, _}

object Streams2 extends App {

  type ProcessT[A] = Process[Task, A]

  val input = Seq("1", "2", "FOO", "3")
  
  val source: ProcessT[String] = Process.emitAll(input)
  val output: Seq[StreamWriter[Task, String, Int]] = input map sum

  /**
  * Converting an Option to a Process[Task, String \/ Int] AKA Writer.
  * */
  def sum(str: String): StreamWriter[Task, String, Int] = {
    parseNumber(str)
    .toRightDisjunction("It's not a number!")
    .point[ProcessT]
    .mapO(_ + 1)
  }

  println(output)

//  def show(str: String) = Task.delay(println(str))
//  def consoleSink: Sink[Task, Int] = Process.constant { n => show(n.toString) }
//
//  output foreach { p =>
//    p.mapO(_ * 2)
//      .mapW("ERROR: ".concat(_))
//      .observeO(consoleSink)
//      .observeW(io.stdOutLines)
//      .run.run
//  }

  /**
  * Mapping the Output to string before perform the product of 2.
  * Observing the Output and the Write side to the the standard output.
  * */
  output foreach { p =>
    p.mapO(n => "VALUE: ".concat((n * 2).toString))
      .mapW("ERROR: ".concat)
      .observeO(io.stdOutLines)
      .observeW(io.stdOutLines)
      .run.run
  }

  def parseNumber(str: String): Option[Int] = Try(str.toInt).toOption

  /**
  * A Channel is just a type alias for Process[Task, String => F[String \/ Int]] in this case
  * */
  val intParser: Channel[Task, String, String \/ Int] = Process.constant { (s: String) =>
    Task.delay {
      import scalaz.syntax.std.string._
      s.parseInt.disjunction.leftMap(_ => s"Failed parsing: $s")
    }
  }

  /**
  * This will return another Process[Any, String \/ Int]
  * */
  val parsed = Process.emitAll(input) through intParser

  //parsed.observeW(Process.eval(Task.delay(println)))
  //print2ln(parsed.runLog.run)

  /**
  * A Writer is a type alias for Process[Task, String \/ Int] in this case
  * */
  val writer: StreamWriter[Task, String, Int] = Process.emitAll(input).toSource through intParser

  /**
  * Mapping the Output and observing the Write side in order to log the errors to the std out.
  * */
  val result = writer
    .mapO(_ * 2)
    .observeW(io.stdOutLines)
    .runLog.run

  print2ln(result)

  case class StreamStats(successes: Int, failures: Int)

  /**
  * A Monoid instance is necessary to accumulate values of type StreamStats
  * */
  implicit val statsMonoid = Monoid.instance[StreamStats] ({ (a, b) =>
    StreamStats(a.successes + b.successes, a.failures + b.failures)
  }, StreamStats(0, 0))

  /**
  * Report StreamStats supported by the Monoid definition and Process.receive1Or that
  * receives only one input or executes the defined Or block
  * */
  def reportStats[A, B](f: StreamStats => Task[Unit]): Process1[A \/ B, A \/ B] = {
    import scalaz.syntax.monoid._
    def go(acc: StreamStats): Process1[A \/ B, A \/ B] = {
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

  /**
  * Piping the Process parsed to the report stats function sending the results to the std out
  * Note that the symbol |> is an alias for 'pipe'
  * */
  val stats = parsed |> reportStats(s => Task.delay(print2ln(s)))
  stats.runLog.run

  def print2ln[A](a: A): Unit = {
    println; println(a)
  }

}
