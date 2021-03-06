import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.util.IntParam

/**
 * Receives text from multiple rawSocketStreams of timestamps, compute
 * timestamp differences, and print the results.
 *
 * Usage: BenchmarkTimestampSerialized <numStreams> <host> <port> <batchMillis> [blockInterval]
 *   <numStream> is the number rawSocketStreams, which should be same as number
 *               of work nodes in the cluster
 *   <host> is the source of the input stream (usually "localhost")
 *   <port> is the port on which the input stream is running on host
 *   <batchMillise> is the Spark Streaming batch duration in milliseconds
 *   [blockInterval] is the Spark Streaming block interval in milliseconds (default is 200)
 */
object BenchmarkTimestampSerialized {
  def main(args: Array[String]) {
    if (args.length != 4 && args.length != 5) {
      System.err.println("Usage: BenchmarkTimestampSerialized <numStreams> <host> <port> <batchMillis> [blockInterval]")
      System.exit(1)
    }

    val (numStreams, host, port, batchMillis) = (args(0).toInt, args(1), args(2).toInt, args(3).toInt)
    val blockInterval = if (args.length == 5) args(4) else None
    val sparkConf = new SparkConf()
    sparkConf.setAppName("BenchMarkTimestamp")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.executor.extraJavaOptions", " -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -XX:+AggressiveOpts -XX:FreqInlineSize=300 -XX:MaxInlineSize=300 ")
    sparkConf.set("spark.eventLog.enabled", "true")
    if (blockInterval != None) sparkConf.set("spark.streaming.blockInterval", blockInterval.toString)
    if (sparkConf.getOption("spark.master") == None) {
      // Master not set, as this was not launched through Spark-submit. Setting master as local."
      sparkConf.setMaster("local[*]")
    }

    // Create the context
    val ssc = new StreamingContext(sparkConf, Duration(batchMillis))

    val times = ssc.rawSocketStream[String](host, port, StorageLevel.MEMORY_ONLY_SER)
    val latencies = times.map{time =>
      val receiveTime = System.currentTimeMillis;
      val sendTime = time.substring(0, 13).toLong;
      val latency = receiveTime - sendTime;
      s"${time.stripLineEnd} $receiveTime $latency"
    }
    //latencies.saveAsTextFiles("latencies")
    latencies.print()
    //latencies.count.map(c => s"$c records").print()

    ssc.start()
    ssc.awaitTermination()
  }
}

