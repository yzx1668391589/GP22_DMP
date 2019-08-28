package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

object TagsChannel extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()


    val row: Row = args(0).asInstanceOf[Row]

    val channel: Int = row.getAs[Int]("adplatformproviderid")

    list:+=("CN"+channel,1)
    list
  }
}
