package com.gvolpe.streams

import scala.language.higherKinds
import scalaz.{\/-, -\/, \/}
import scalaz.concurrent.Task
import scalaz.stream._

object StreamsApp extends App with StreamOneSourceTwoSinks with StreamParallelism with StreamDebug {

  //streamTwoOutputs.run
  //flowObservedWithDebug.run.run
  //concurrentFlow2.run.run
  //debug(parallel).run.run
  debug(parallelProcess).run.run

}

trait StreamDebug {
  import StreamTypes._
  def debug[A](stream: Stream[Task, A]): Stream[Task, String] =
    stream map {a => s"debug: $a" } observe io.stdOutLines
}

// Defined alias types for Sink and Source. In general, F is always a Task
object StreamTypes {
  type Sink[F[_], A] = Process[F, A => F[Unit]]
  type Source[F[_], A] = Process[F, A]
  type Stream[F[_], A] = Process[F, A]
}

trait StreamOneSourceOneSink {
  import StreamTypes._

  // Execute the Task somewhere in time (part of the stream processing)
  def write(str: String): Task[Unit] = Task.delay { println(str) }

  // Create implementations of source and sink
  val src: Source[Task, String] = Process.range(0, 10) map (n => (n + 1).toString)
  val stdOut: Sink[Task, String] = Process.constant(write _)

  // Connect source, process and sink
  val flow: Stream[Task, Unit] = src zip stdOut flatMap {
    case (str, f) => Process eval f(str)
  }

  // Prepare the streaming to be executed
  val stream: Task[Unit] = flow.run

}

trait StreamOneSourceTwoSinks extends StreamOneSourceOneSink {
  self: StreamDebug =>
  import StreamTypes._

  def writeAppendDate(str: String): Task[Unit] = Task.delay { println(s"# ${str} on ${new java.util.Date}") }

  // Secondary Sink
  val channel: Sink[Task, String] = Process.constant(writeAppendDate _)

  // Connect source, process and all the sinks
  val flowTwoOutputs: Stream[Task, Unit] = src zip stdOut zip channel flatMap {
    case ((str, f1), f2) => for {
      _ <- Process eval f1(str)
      _ <- Process eval f2(str)
    } yield ()
  }

  // Same as flowTwoOutputs but much more elegant
  val flowObserved: Stream[Task, Unit] = src observe stdOut to channel
  val flowObservedWithDebug: Stream[Task, String] = debug(src observe channel)

  // Prepare the streaming to be executed
  val streamTwoOutputs: Task[Unit] = flowObserved.run
}

trait StreamConcurrency extends StreamOneSourceOneSink {
  import StreamTypes._

  case class StreamMessage(value: String)

  val left: Source[Task, StreamMessage] = Process.range(0, 10) map (n => StreamMessage(s"left src: ${n.toString}"))
  val right: Source[Task, StreamMessage] = Process.range(0, 10) map (n => StreamMessage(s"right src: ${n.toString}"))

  val mergedSrc: Source[Task, StreamMessage] = left merge right // Same as: left.wye(right)(wye.merge)

  val streamMessageSink: Sink[Task, StreamMessage] = Process.constant(m => write(m.value))

  val concurrentFlow: Stream[Task, Unit] = mergedSrc to streamMessageSink

}

trait StreamConcurrencyDifferentTypes extends StreamConcurrency {
  import StreamTypes._

  type StreamData = StreamMessage \/ StreamNumber

  case class StreamNumber(value: Int)

  val numbers: Source[Task, StreamNumber] = Process.range(0, 10) map StreamNumber.apply
  val streamDataSink: Sink[Task, StreamData] = Process.constant {
    case -\/(message) => write(message.value)
    case \/-(number) => write(s"# ${number.value}")
  }

  val mergedDifferentSrc: Source[Task, StreamData] = left either numbers // Same as: left.wye(numbers)(wye.either)

  val concurrentFlow2: Stream[Task, Unit] = mergedDifferentSrc to streamDataSink
}

trait StreamParallelism extends StreamConcurrencyDifferentTypes {
  import StreamTypes._

  def numbersTask(number: StreamNumber): Task[Int] = Task.delay { number.value * 10 }
  def numbersProcessor(number: StreamNumber): Stream[Task, Int] = Process.eval(Task.delay { number.value * 10 })

  val parallelSource: Source[Task, Task[Int]] = numbers map { n => numbersTask(n) }
  val parallelSource2: Source[Task, Stream[Task, Int]] = numbers map { n => numbersProcessor(n) }

  // Get 4 messages at a time and parallelizes
  // Gather causes DEADLOCK on infinite streams! (eg:  a queue)
  val parallel: Process[Task, Int] = parallelSource.gather(4)

  // Best option for parallelization. Runs faster as it can by racing all input streams.
  val parallelProcess: Process[Task, Int] = merge.mergeN(parallelSource2)
}

trait StreamChatSimulation extends StreamParallelism {
  import StreamTypes._

  // Good way to define a queue with multiple consumers like in Kafka
  val queue = async.topic[String]() // Better use scodec.bits.BitVector for a real case

  val in: Stream[Task, Unit] = src to queue.publish
  val out: Stream[Task, Unit] = queue.subscribe to stdOut
  val connection: Stream[Task, Unit] = in merge out
  val handlers: Stream[Task, Stream[Task, Unit]] = Process.eval(Task.delay(connection))

  val chatServer = merge.mergeN(handlers).run
}
