package com.Rpt

import com.utils.RptUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

object NetworkRpt {
  def main(args: Array[String]): Unit = {
    // 创建一个集合保存输入和输出目录
    val Array(inputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    val df: DataFrame = sQLContext.read.parquet(inputPath)


    /**
      * sparkSQL方式
      */

//        df.createTempView("logs")
//        sQLContext.sql("select networkmannername," +
//          "sum(case when requestmode=1 and processnode >=1 then 1 else 0 end) origin," +
//          "sum(case when requestmode=1 and processnode >=2 then 1 else 0 end) valid," +
//          "sum(case when requestmode=1 and processnode =3 then 1 else 0 end) ad," +
//          "sum(case when iseffective=1 and isbilling =1 and isbid=1 then 1 else 0 end) joiner," +
//          "sum(case when iseffective=1 and isbilling =1 and adorderid !=0 then 1 else 0 end) suc," +
//          "sum(case when requestmode=2 and iseffective =1 then 1 else 0 end) shower," +
//          "sum(case when requestmode=3 and iseffective =1 then 1 else 0 end) cli," +
//          "sum(case when iseffective=1 and isbilling =1 and iswin =1  then winprice/1000.0 else 0 end) DSPcon," +
//          "sum(case when iseffective=1 and isbilling =1 and iswin =1 then adpayment/1000.0 else 0 end) DSPcos " +
//          "from logs group by networkmannername").createTempView("log")
//
//        sQLContext.sql("select networkmannername,origin,valid,ad,joiner,suc," +
//          "(joiner/suc) as sucr,shower,cli,(cli/shower) as clir,DSPcos,DSPcon from log").show()


    /**
      * SparkCore方式
//      */
    val value: RDD[String] = df.rdd.map(row => {
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val WinPrice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")
      val networkmannername: String = row.getAs[String]("networkmannername")
      val reqlist = RptUtils.request(requestmode, processnode)
      val clicklist = RptUtils.click(requestmode, iseffective)
      val allist = RptUtils.bidding(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)
      (networkmannername, reqlist ++ clicklist ++ allist)
    }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }).map(t => {
      t._1 + "," + t._2.mkString(",")
    })

    value.collect.toBuffer.foreach(println)

//            val rowRDD: RDD[Row] = value.map(_.split(",")).map(row => {
//              Row(
//                row(0),
//                row(1),
//                row(2),
//                row(3),
//                row(4),
//                row(5),
//                row(6),
//                row(7),
//                row(8),
//                row(9),
//                row(10)
//              )
//            })
//            val DF: DataFrame = sQLContext.createDataFrame(rowRDD,SchemaUtils2.structtype)
//                val conn: Properties = new Properties()
//                conn.setProperty("user","root")
//                conn.setProperty("password","123456")
//                DF.write.jdbc("jdbc:mysql://localhost:3306/movie?useSSL=false","c1",conn)



    sc.stop()
  }
}
