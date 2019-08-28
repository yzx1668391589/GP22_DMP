package com.Tags

import com.utils.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsKeywords extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String,Int)]()

    var row =args(0).asInstanceOf[Row]

    val stopword: Broadcast[Map[String, String]] = args(1).asInstanceOf[Broadcast[Map[String,String]]]
    //关键字标签
    val str = row.getAs[String]("keywords")

//    if(str.contains("|")){
      val strs: Array[String] = str.split("\\|")

    strs.filter(word => {
      word.length >= 3 && word.length <= 8 && stopword.value.contains(word)
    }).foreach(word=>list:+=("K"+word,1))

    list
  }
}
