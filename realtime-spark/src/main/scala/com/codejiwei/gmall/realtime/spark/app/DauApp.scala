package com.codejiwei.gmall.realtime.spark.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.codejiwei.gmall.realtime.spark.util.{MyKafkaUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object DauApp {
  def main(args: Array[String]): Unit = {

    //TODO 1 创建sparkstreaming 环境

    val conf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
    val ssc = new StreamingContext(conf, Seconds(5))
    val groupId = "dau_app_group"
    val topic = "ODS_BASE_LOG"


    //TODO 2 从kafka接收数据

    val recordInputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

    //TODO 3 处理数据 转换格式、筛选、去重

    //    recordInputDStream.map(_.value()).print()

    val jsonObjDStream: DStream[JSONObject] = recordInputDStream.map { record => {

      //获取启动日志
      val jsonStr: String = record.value()
      val jsonObject: JSONObject = JSON.parseObject(jsonStr)
      val ts = jsonObject.getLong("ts")

      //获取字符串  日期 小时
      val dateHourString = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
      val dateHour: Array[String] = dateHourString.split(" ")
      jsonObject.put("dt", dateHour(0))
      jsonObject.put("hr", dateHour(1))

//      println(jsonObject)
      jsonObject
    }
    }

    //TODO 筛选出 用户首次访问的页面
    val firstVisitDStream: DStream[JSONObject] = jsonObjDStream.filter(jsonObj => {
      var ifFirst = false
      //从传来的一个一个的JSONObject中找到含有key=“page”的。
      val pageJson: JSONObject = jsonObj.getJSONObject("page")
      //有key = “page”的，再去判断是否有last_page_id。
      if (pageJson != null) {

        //从含有key="page"的，中找是否含有“last_page_id”
        val lastPageId: String = pageJson.getString("last_page_id")
        //TODO 保留 含有page，并且lastPageId=null或长度为0的数据。
        if (lastPageId == null || lastPageId.length == 0) {
          ifFirst = true //true的保留
        }
      }
      ifFirst
    })

//    firstVisitDStream.print(100)

    //TODO 去重，去掉那些：在不同批次中出现的相同mid
    //TODO 为什么不用checkpoint？用的是Redis
//    firstVisitDStream.filter { jsonObj => {
//
//      val jedis = MyRedisUtil.getJedisClient
//      //统计日货：redis用什么结构去存呢？key = 2020-12-18 ；value =mid；expire = 24 * 3600
//      //去重：sadd
//      val mid: String = jsonObj.getJSONObject("common").getString("mid")
//      val dt = jsonObj.getString("dt")
//      val dauKey = "dau" + dt
//      val nonExists = jedis.sadd(dauKey, mid) //返回0 表示已存在，返回1 表示未存在
//      jedis.expire(dauKey, 24 * 3600)
//      jedis.close()
//      //如果不存在
//      if (nonExists == 1L) {
//        true //那就保留
//      } else {
//        false //如果存在了，那就过滤掉
//      }
//    }
//    }


    //TODO 上面的这种方式有一个问题：在一个时间批次中，每条数据都获取一个连接池
    val dauJsonObjectDStream: DStream[JSONObject] = firstVisitDStream.mapPartitions { jsonIter =>{
      val jedis = MyRedisUtil.getJedisClient
      val sourceList = jsonIter.toList
      println("未筛选前：" + sourceList.size)

      val rsList = new ListBuffer[JSONObject]()

      //遍历每个时间批次的迭代器
      for (jsonObj <- sourceList) {
        val mid = jsonObj.getJSONObject("common").getString("mid")
        val dt = jsonObj.getString("dt")
        val dauKey = "dau:" + dt
        val nonExists = jedis.sadd(dauKey, mid)
        jedis.expire(dauKey, 24 * 3600)
        if (nonExists == 1L) {
          //如果不存在那就添加到结果rsList
          rsList.append(jsonObj)
        }
      }

      //关闭资源
      jedis.close()
      println("筛选后：" + rsList.size)

      rsList.toIterator
    }}

    //TODO 4 输出数据
    dauJsonObjectDStream.print(100)

    //
    ssc.start()
    ssc.awaitTermination()

  }
}
