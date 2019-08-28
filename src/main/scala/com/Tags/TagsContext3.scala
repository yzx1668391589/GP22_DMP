package com.Tags

import com.utils.TagsUtils
import org.apache.hadoop.hbase.client.{ConnectionFactory}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object TagsContext3 {
  def main(args: Array[String]): Unit = {
    //    if(args.length != 4){
    //      println("目录不匹配，退出程序")
    //      sys.exit()
    //    }

    val Array(inputPath, dirPath, stopPath, days) = args

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
    configuration.set("hbase.zookeeper.quorum", "hadoop01:2181")
    //      ,load.getString("hbase.host"))

    //创建Hbase连接
    val hbconn = ConnectionFactory.createConnection(configuration)

    val hbadmin = hbconn.getAdmin
    //判断表是否可用
    if (!hbadmin.tableExists(TableName.valueOf(hbaseTableName))) {
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
    jobconf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)


    //读取数据
    val df = sQLContext.read.parquet(inputPath)
    val dir = sc.textFile(dirPath)

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

    val broadcast = sc.broadcast(map)

    //获取停用词库
    val stopword: Map[String, Int] = sc.textFile(stopPath).map((_, 0)).collect.toMap

    val stopmap = sc.broadcast(stopword)


    //过滤符合ID的数据
    val baseRDD = df.filter(TagsUtils.OneUserId)
      .rdd
      //接下来所有的标签都在内部实现
      .map(row => {
      //取出用户ID
      val userList = TagsUtils.getAllUserId(row)
      (userList, row)
    })

    val vertiesRDD: RDD[(Long, List[(String, Int)])] = baseRDD.flatMap(tp => {
      val row = tp._2
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
      val AllTag = adList ++ appList ++ channelList ++ equipmentList ++ keywordsList ++ LocationList ++ business
      //保证其中一个点携带标签，同时也保留所有点
      val VD = tp._1.map((_, 0)) ++ AllTag
      //处理所有的点的集合
      tp._1.map(uid => {
        //保证一个点携带标签
        if (tp._1.head.equals(uid)) {
          (uid.hashCode.toLong, VD)
        } else {
          (uid.hashCode.toLong, List.empty)
        }
      })
    })

    //构建变的集合
    val edges: RDD[Edge[Int]] = baseRDD.flatMap(tp => {
      tp._1.map(uid => Edge(tp._1.head.hashCode, uid.hashCode, 0))
    })
    //构成图
    val graph = Graph(vertiesRDD,edges)
    //取出顶点
    val vertices = graph.connectedComponents().vertices
    vertices.join(vertiesRDD).map{
      case(uid,(conId,tagsAll))=>{(conId,tagsAll)}
    }.reduceByKey((list1,list2)=>{
      //聚合所有的标签
      (list1++list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    }).take(20).foreach(println)


    sc.stop()
  }
}
