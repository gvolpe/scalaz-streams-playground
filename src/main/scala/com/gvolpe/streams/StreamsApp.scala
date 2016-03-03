package com.gvolpe.streams

import scala.language.higherKinds
import scalaz.concurrent.Task
import scalaz.stream._

object StreamsApp extends App with StreamOneSourceTwoSinks {

  streamTwoOutputs.run

}

// Defined alias types for Sink and Source. In general, F is always a Task
trait StreamTypes {
  type Sink[F[_], A] = Process[F, A => F[Unit]]
  type Source[F[_], A] = Process[F, A]
}

trait StreamOneSourceOneSink extends StreamTypes {

  // Execute the Task somewhere in time (part of the stream processing)
  def write(str: String): Task[Unit] = Task.delay { println(s"# ${str}") }

  // Create implementations of source and sink
  val src: Source[Task, String] = Process.range(0, 10) map (n => (n + 1).toString)
  val sink: Sink[Task, String] = Process.constant(write _)

  // Connect source, process and sink
  val flow: Process[Task, Unit] = src zip sink flatMap {
    case (str, f) => Process eval f(str)
  }

  // Prepare the streaming to be executed
  val stream: Task[Unit] = flow.run

}

trait StreamOneSourceTwoSinks extends StreamOneSourceOneSink {

  def writeAppendDate(str: String): Task[Unit] = Task.delay { println(s"# ${str} on ${new java.util.Date}") }

  // Secondary Sink
  val channel: Sink[Task, String] = Process.constant(writeAppendDate _)

  // Connect source, process and all the sinks
  val flowTwoOutputs: Process[Task, Unit] = src zip sink zip channel flatMap {
    case ((str, f1), f2) => for {
      _ <- Process eval f1(str)
      _ <- Process eval f2(str)
    } yield ()
  }

  // Prepare the streaming to be executed
  val streamTwoOutputs: Task[Unit] = flowTwoOutputs.run

}
