import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object Producer1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)


    val ssc = new StreamingContext(sc,Durations.seconds(5))
    val logs: ReceiverInputDStream[String]= ssc.socketTextStream("hadoop01", 6666)

    val res: DStream[(String, Int)] = logs.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    res.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
