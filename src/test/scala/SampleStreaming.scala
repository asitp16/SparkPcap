/**
 * Created by aparija on 12/5/15.
 */



import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark._
import org.apache.spark.sql._
import java.util.Date
import java.nio.file.Paths
import java.text.SimpleDateFormat

import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.hadoop.mapred.{TextInputFormat, FileInputFormat, JobConf}

import com.mapr.sample.WholeFileInputFormat
import edu.gatech.sjpcap._
import scala.collection.mutable.ListBuffer

object SampleStreaming {
  //case class FlowData(timestampMillis: Long, srcIP: String, dstIP: String, srcPort: Integer, dstPort: Integer, protocol: String, length: Integer, payload: Array[Byte], captureFilename: String)
  case class pcapacket(timestampMillis: Long,srcIP: String, dstIP: String)
  def main(args: Array[String]): Unit =  {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(20))
    val inputPath = "/Users/aparija/Documents/mapr-pcapstream/input"
    val outPath = "/Users/aparija/Documents/mapr-pcapstream/output"
    val directoryFormat = new SimpleDateFormat("'flows'/yyyy/MM/dd/HH/mm/ss")


    val pcapBytes = ssc.fileStream[NullWritable, BytesWritable,WholeFileInputFormat](directory = inputPath)
    val pcapList = pcapBytes.flatMap{
     case(x,y) =>
       val pcapParser = new PcapParser()
       pcapParser.openFile(y.getBytes)
       var packet  = pcapParser.getPacket()
       var list = new ListBuffer[Packet]()
       while(packet != Packet.EOF) {
         if (!(packet.isInstanceOf[IPPacket])) {
           packet = pcapParser.getPacket();
         }
         else {
           list += packet
           packet = pcapParser.getPacket()
         }
       }
       pcapParser.closeFile()
       list
   }
    val sc = ssc.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    pcapList.foreachRDD(rdd => {
      val date = new Date()
      val out = Paths.get(outPath, directoryFormat.format(date)).toString
      rdd.map(x => {
        val ipPacket = x.asInstanceOf[IPPacket]
        pcapacket(ipPacket.timestamp, ipPacket.src_ip.getHostAddress(), ipPacket.dst_ip.getHostAddress())
      }).toDF().write.parquet(out)
    })



    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate


  }

}
