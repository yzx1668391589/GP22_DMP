package com.utils

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * 字典文件处理
  */
object DictionaryUtils {
  def main(args: Array[String]): Map[String, String] = {
    // 创建一个集合保存输入和输出目录
    val Array(inputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")

    val lines = sc.textFile(inputPath)

    val line: RDD[(String, String)] = lines.map(x => x.replace(" ", "-"))
      .map(_.split("\t")).filter(_.length > 5).map(x => {
      if (x(3).contains(".")) {
        Tuple2(x(3), x(1))
      } else if (x(4).contains(".")) {
        Tuple2(x(4), x(1))
      } else {
        Tuple2(x(5), x(1))
      }
    })

    val map: Map[String, String] = line.collect.toMap

//
//    val line: RDD[(String, String)] = lines.map(_.split("\\s+")).filter(_.length > 5).map(x => {
//      if (x(3).contains(".")) {
//        Tuple2(x(3), x(1))
//      } else if (x(4).contains(".")) {
//        Tuple2(x(4), x(1))
//      } else {
//        Tuple2(x(5), x(1))
//      }
//    })


    line.collect.toBuffer.foreach(println)


    sc.stop()
    map
  }

}
