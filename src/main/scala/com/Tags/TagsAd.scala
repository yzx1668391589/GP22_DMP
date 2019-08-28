package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 广告标签
  */
object TagsAd extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()
    //解析参数
    var row =args(0).asInstanceOf[Row]

    //获取广告类型和广告类型名称
    val adType: Int = row.getAs[Int]("adspacetype")
    adType match {
      case v if v > 9 => list:+=("LC"+v,1)
      case v if v <= 9 && v > 0 => list:+=("LC0"+v,1)
    }

    val adName: String = row.getAs[String]("adspacetypename")
    if(StringUtils.isNoneBlank(adName)){
      list:+=("LN"+adName,1)
    }

    list
  }
}
