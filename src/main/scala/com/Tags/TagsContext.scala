package com.Tags

import com.utils.TagsUtils
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
object TagsContext {
  def main(args: Array[String]): Unit = {
//    if(args.length != 4){
//      println("目录不匹配，退出程序")
//      sys.exit()
//    }

    val Array(inputPath,dirPath,stopPath,days)=args

    //创建上下文
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    //todo 加载配置文件
//    val load: Config = ConfigFactory.load("ConfProperties.conf")

    //表名称
    val hbaseTableName = "gp22"
//      load.getString("hbase.TableName")

    //创建任务
    val configuration = sc.hadoopConfiguration

    //设置对应的参数
    configuration.set("hbase.zookeeper.quorum","hadoop01:2181")
//      ,load.getString("hbase.host"))

    //创建Hbase连接
    val hbconn = ConnectionFactory.createConnection(configuration)

    val hbadmin = hbconn.getAdmin
    //判断表是否可用
    if(!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
      //创建表的操作
      val tableDesc = new HTableDescriptor(TableName.valueOf(hbaseTableName))

      //创建列簇
      val descriptor = new HColumnDescriptor("tags")

      //将列簇加到表中
      tableDesc.addFamily(descriptor)

      //表加到admin中
      hbadmin.createTable(tableDesc)

      hbadmin.close()
      hbconn.close()
    }

    //创建相关JobCOnf
    val jobconf = new JobConf(configuration)

    //指定输出类型和表类型
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)


    //读取数据
    val df = sQLContext.read.parquet(inputPath)
    val dir= sc.textFile(dirPath)

    val map: Map[String, String] = dir.map(x => x.replace(" ", "-"))
      .map(_.split("\t")).filter(_.length > 5).map(x => {
      if (x(3).contains(".")) {
        Tuple2(x(3), x(1))
      } else if (x(4).contains(".")) {
        Tuple2(x(4), x(1))
      } else {
        Tuple2(x(5), x(1))
      }
    }).collect.toMap

    val broadcast  = sc.broadcast(map)


    val stopword: Map[String, Int] = sc.textFile(stopPath).map((_,0)).collect.toMap

    val stopmap  = sc.broadcast(stopword)


    //过滤符合ID的数据
    df.filter(TagsUtils.OneUserId)
      .rdd
      //接下来所有的标签都在内部实现
      .map(row => {
      //取出用户ID
      val userId = TagsUtils.getOneUserId(row)

      //接下来通过row数据  打上  所有标签  （按照需求）

      //广告标签
      val adList: List[(String, Int)] = TagsAd.makeTags(row)

      //app名称标签
      val appList: List[(String, Int)] = TagsApp.makeTags(row, broadcast)

      //渠道标签
      val channelList: List[(String, Int)] = TagsChannel.makeTags(row)

      //操作系统标签
      val equipmentList: List[(String, Int)] = TagsEquipment.makeTags(row)

      //关键字标签
      val keywordsList: List[(String, Int)] = TagsKeywords.makeTags(row, stopmap)

      //地域标签
      val LocationList: List[(String, Int)] = TagsLocation.makeTags(row)

      //商圈标签
      val business: List[(String, Int)] = TagsBusiness.makeTags(row)


      (userId, adList ++ appList ++ channelList ++ equipmentList ++ keywordsList ++ LocationList ++ business)

    }).reduceByKey((list1, list2) => (list1 ::: list2).groupBy(_._1).mapValues(_.foldLeft[Int](0)(_ + _._2)).toList)
        .map{
          case(userid,userTag)=>{
            val put = new Put(Bytes.toBytes(userid))

            //处理标签
            val tags=userTag.map(t=>t._1+","+t._2).mkString(",")
            put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(s"$days"),Bytes.toBytes(tags))
            (new ImmutableBytesWritable(),put)
          }
        }.saveAsHadoopDataset(jobconf)


  }

}
