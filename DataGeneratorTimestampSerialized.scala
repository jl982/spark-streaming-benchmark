import java.io.{ByteArrayOutputStream, IOException}
import java.net.ServerSocket
import java.nio.ByteBuffer
import java.util.Arrays

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer

/**
 * A helper program that sends timestamps one at a time in Kryo serialized format at a specified rate.
 *
 * Usage: DataGeneratorTimestampSerialized <port> <bytesPerSec>
 *   <port> is the port on localhost to run the generator
 *   <bytesPerSec> is the number of bytes the generator will send per second
 */
object DataGeneratorTimestampSerialized {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: DataGeneratorTimestampSerialized <port> <bytesPerSec> <recordsPerBlock>")
      System.exit(1)
    }
    // Parse the arguments using a pattern match
    val (port, bytesPerSec, recordsPerBlock) = (args(0).toInt, args(1).toInt, args(2).toInt)

    val bufferStream = new ByteArrayOutputStream(32)
    val ser = new KryoSerializer(new SparkConf()).newInstance()
    val serStream = ser.serializeStream(bufferStream)

    val serverSocket = new ServerSocket(port)
    println("Listening on port " + port)

    while (true) {
      val socket = serverSocket.accept()
      println("Got a new connection")
      val out = new RateLimitedOutputStream(socket.getOutputStream, bytesPerSec)
      try {
        var counter = 0
        var accum: Long = 0
        while (true) {
          val curTimeString = System.currentTimeMillis.toString
          bufferStream.reset()
          for (i <- 0 until recordsPerBlock) {
            serStream.writeObject(curTimeString + "-" + counter.toString + "\n")
            counter += 1
          }
          serStream.flush()
          val array = bufferStream.toByteArray

          val countBuf = ByteBuffer.wrap(new Array[Byte](4))
          countBuf.putInt(array.length)
          countBuf.flip()

          //println("array.length [" + array.length + "]")
          //println("countBuf.array [" + Arrays.toString(countBuf.array) + "]")
          //println("array [" + Arrays.toString(array) + "]")
          out.write(countBuf.array)
          out.write(array)

          accum += System.currentTimeMillis - curTimeString.toLong
          if (counter % 1000000 == 0) {
            println(accum + " " + counter + " " + accum.toDouble/(counter/recordsPerBlock))
          }
        }
      } catch {
        case e: IOException =>
          println("Client disconnected.")
          socket.close()
      }
    }
  }
}

