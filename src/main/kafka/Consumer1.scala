import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{ Seconds, StreamingContext}

object Consumer1 {  def main(args: Array[String]): Unit = {
  val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
  //    val sc = new SparkContext(conf)
  val ssc = new StreamingContext(conf,Seconds(5))

  ssc.checkpoint("d://2222")


  val logs = ssc.socketTextStream("hadoop01",666)

  val tups = logs.flatMap(_.split(" ")).map((_,1))

  val res = tups.updateStateByKey(func,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)

  res.print()

  ssc.start()
  ssc.awaitTermination()
}

  val func=(it:Iterator[(String,Seq[Int],Option[Int])])=>{
    it.map(tup=>{
      (tup._1,tup._2.sum+tup._3.getOrElse(0))
    })
  }
}

