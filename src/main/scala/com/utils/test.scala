package com.utils

import com.Tags.TagsBusiness
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    val df=sQLContext.read.parquet("D:\\all")
    df.rdd.map(row=>{
      val business=TagsBusiness.makeTags(row)
      business
    }).foreach(println)

  }

}
