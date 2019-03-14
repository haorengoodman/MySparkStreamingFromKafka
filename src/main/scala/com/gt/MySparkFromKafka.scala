package com.gt

import java.{lang, util}

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * SparkStreaming 从 kafka 获取数据
  */
object MySparkFromKafka {
  val appName = "appName"
  val master = "local[2]"
  val topic_1 = "foobar"
  val groupId = "group_gt"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(appName).setMaster(master)
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "100")//那么每秒最大吞吐就是100条
    val ssc = new StreamingContext(conf, Seconds(10))

    //Direct 方式是直接连接到kafka的节点上获取数据
    //1. 获取topic列表
    val topics = getTopicList()
    //2. 获取kafka配置参数
    val kafkaParams = getKafkaParams()
    //3. 从外部存储中获取topic每一个分区的偏移量，并都贱offsets集合
    val offsetMap = getOffsetsFromZk(topic_1)
    //4. 创建 subscribe(订阅)
    val subscribe = ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsetMap)
    //5. 创建 InputDStream
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, subscribe)
    //6. 数据处理
    stream.foreachRDD(dataExecuter)
    //7. 启动
    ssc.start()
    ssc.awaitTermination()
  }

  val dataExecuter = (rdd:RDD[ConsumerRecord[String,String]]) =>{
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    rdd.foreachPartition(it =>{
      it.foreach(record =>{
        val key = record.key()
        val value = record.value()
        println(key,value)
      })
      //更新偏移量到zk
      val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
      val zkNodePath = "/"+o.topic+"_offset_"+groupId+"/"+o.partition
      MyZkUtil.updateZkNodeData(zkNodePath,o.untilOffset.toString)
    })
  }

  /** 获取topic 集合 */
  def getTopicList(): List[String] = {
    List(topic_1) //List("topic1","topic2")
  }

  /** 获取kafka配置参数 */
  def getKafkaParams(): Map[String, String] = {
    val kafkaPram = Map(
      "bootstrap.servers" -> "k0001.xh-kafka.com:9092,k0002.xh-kafka.com:9092,k0003.xh-kafka.com:9092,k0004.xh-kafka.com:9092,k0005.xh-kafka.com:9092",
      "auto.offset.reset" -> "latest",
      "group.id" -> groupId,
      "enable.auto.commit" -> "false",
      "auto.commit.interval.ms" -> "1000",
      "session.timeout.ms" -> "30000",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaPram
  }

  //TODO 从zk 中获取topic的各个分区以及偏移量信息
  def getOffsetsFromZk(topicName: String): Map[TopicPartition, Long] = {
    //从kafka中获取到的偏移量信息
    val offsets = MyZkUtil.getEarliestOffset(topicName)
    import scala.collection.JavaConversions._
    val set = offsets.entrySet()
    var offsetMap: Map[TopicPartition, Long] = Map()//偏移量最终结果集
    for(entity <- set){
      val topicPartition: TopicPartition = entity.getKey() //分区
      val kafka_offset:lang.Long = entity.getValue()   //偏移量
      val zkNodePath = "/"+topicName+"_offset_"+groupId+"/"+topicPartition.partition()
      val flag = MyZkUtil.ensureZkPathExists(zkNodePath)
      if(flag){
        val zkNodeData = MyZkUtil.getZkNodeData(zkNodePath)
        if(zkNodeData != null){
          val zk_offset = Integer.parseInt(zkNodeData)
          if(zk_offset >= kafka_offset){
            offsetMap += (topicPartition -> zk_offset)
          }else{
            offsetMap += (topicPartition -> kafka_offset)
          }
        }
      }else{
        offsetMap += (topicPartition -> kafka_offset)
      }
    }
    offsetMap
  }

}
