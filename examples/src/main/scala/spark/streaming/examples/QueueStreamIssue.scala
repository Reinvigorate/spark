package spark.streaming.examples

import spark.RDD
import spark.streaming.{Seconds, StreamingContext}
import spark.streaming.StreamingContext._

import scala.collection.mutable.SynchronizedQueue

import org.apache.log4j.Level
import org.apache.log4j.Logger


object QueueStreamIssue {
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: QueueStream <master>")
      System.exit(1)
    }
    
    Logger.getRootLogger().setLevel(Level.WARN);

    // Create the context
    val ssc = new StreamingContext(args(0), "QueueStream", Seconds(1))

    // Create the queue through which RDDs can be pushed to 
    // a QueueInputDStream
    val rddQueue = new SynchronizedQueue[RDD[Int]]()
    
    // Create the QueueInputDStream and use it do some processing
    val inputStream = ssc.queueStream(rddQueue)
    val mappedStream = inputStream.map(x => x -> x)


    val updateStateFunc = (events: Seq[Int], state: Option[Int]) => {

      val v = state.getOrElse(events.head)
      println("Called on: " + v)
      Option(v)
    }

    val stateStream = mappedStream.updateStateByKey[Int](updateStateFunc)
    stateStream.print()    
    ssc.start()
    
    // Create and push some RDDs into
	for (i <- 1 to 3) {
      rddQueue += ssc.sc.makeRDD(Seq(i), 10)
      Thread.sleep(1000)
    }

    Thread.sleep(4000)

    rddQueue += ssc.sc.makeRDD(Seq(4), 10)

    Thread.sleep(20000)


    ssc.stop()
    System.exit(0)
  }
}