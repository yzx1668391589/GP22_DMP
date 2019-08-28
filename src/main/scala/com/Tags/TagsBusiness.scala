package com.Tags

import ch.hsr.geohash.GeoHash
import com.ETL.Utils2Type
import com.utils.{AmapUtils, Tag}
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object TagsBusiness extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()

    val row: Row = args(0).asInstanceOf[Row]

    val long =Utils2Type.toDouble(row.getAs[String]("long"))
    val lat =Utils2Type.toDouble(row.getAs[String]("lat"))

    if(long >=73 && long <= 135 && lat >=3 && lat <=54){
      val business = getBusiness(long,lat)

      if(StringUtils.isNotBlank(business)){
        val lines = business.split(",")

        lines.foreach(f=>list:+=(f,1))

      }

    }

    /**
      * 获取商圈信息
      */
    def getBusiness(long:Double,lat:Double): String ={
      //转换GeoHash字符串
      val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat,long,8)

      var business = redis_queryBusiness(geohash)

      if(business==null || business.length==0){
        business=AmapUtils.getBussAmap(long,lat)
        redis_insertBusiness(geohash,business)
      }
      business
    }

    def redis_queryBusiness(geohash:String):String={
      val jedis =new Jedis("hadoop01",6379);
      val business = jedis.get(geohash)
      jedis.close()
      business
    }

    def redis_insertBusiness(geohash: String,business: String)={
      val jedis =new Jedis("hadoop01",6379);
      jedis.set(geohash,business)
      jedis.close()
    }




    list
  }
}
