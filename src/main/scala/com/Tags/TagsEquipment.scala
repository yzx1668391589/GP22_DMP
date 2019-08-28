package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

object TagsEquipment extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String,Int)]()

    var row =args(0).asInstanceOf[Row]

    //操作系统
    val client=row.getAs[Int]("client")

    client match {
      case 1 =>list:+=("D00010001",1)
      case 2 =>list:+=("D00010002",1)
      case 3 =>list:+=("D00010003",1)
      case _ =>list:+=("D00010004",1)
    }

    val network =row.getAs[String]("networkmannername")
    network match {
      case "WIFI" =>list:+=("D00020001",1)
      case "4G" =>list:+=("D00020002",1)
      case "3G" =>list:+=("D00020003",1)
      case "2G" =>list:+=("D00020004",1)
      case _ =>list:+=("D00020005",1)
    }
    //设备运营商
    val ispname=row.getAs[String]("ispname")
    ispname match {
      case "移动" =>list:+=("D00030001",1)
      case "联通" =>list:+=("D00030002",1)
      case "电信" =>list:+=("D00030003",1)
      case _ =>list:+=("D00030004",1)
    }


    list

  }
}
