package com.Tags

import com.utils.{DictionaryUtils, Tag}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import redis.clients.jedis.Jedis

object TagsApp extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {

    val jedis=new Jedis("hadoop01",6379)
    var list = List[(String,Int)]()
//
//    var row =args(0).asInstanceOf[Row]
//
//    val appmap = args(1).asInstanceOf[Broadcast[Map[String,String]]]
//
//    val appname: String = row.getAs[String]("appname")
//    val appid: String = row.getAs[String]("appid")
//
//    if(StringUtils.isNotBlank(appname)){
//      list:+=("APP"+appname,1)
//    }else if(StringUtils.isNotBlank(appid)){
//      list:+("APP"+appmap.value.getOrElse(appid,appid),1)
//    }

    var row =args(0).asInstanceOf[Row]


    val appname: String = row.getAs[String]("appname")
    val appid: String = row.getAs[String]("appid")

    if(StringUtils.isNotBlank(appname)){
      list:+=("APP"+appname,1)
    }else if(StringUtils.isNotBlank(appid)){
      list:+("APP"+jedis.get(appid),1)
    }

    list
  }
}
