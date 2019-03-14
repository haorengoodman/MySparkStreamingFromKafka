package com.gt

import java.{lang, util}
import java.util.{Map, Properties}

import com.gt.MySparkFromKafka.groupId
import org.apache.curator.framework.api.ExistsBuilder
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooDefs}

/**
  * zk 工具类
  *
  * Versions
  * The are currently two released versions of Curator, 2.x.x and 3.x.x:
  * Curator 2.x.x - compatible with both ZooKeeper 3.4.x and ZooKeeper 3.5.x
  * Curator 3.x.x - compatible only with ZooKeeper 3.5.x and includes support for new features such as dynamic reconfiguration, etc
  */
object MyZkUtil {
  val zkPath = "/article_resource_offset_interfaceTest"
  private var curator: CuratorFramework = null

  def main(args: Array[String]): Unit = {
    //val flag = ensureZkPathExists(zkPath)
    //print(flag)
    //createZkNode()
    //deleteZkNode()
    //deleteZkNodes()
    //getZkNodeData()
    //getZkNodeDataByWatch()
    /*getZkNodeData()
    updateZkNodeData()
    getZkNodeData()*/

    getEarliestOffset("article_resource")

    closeCurator()
  }

  //更新节点数据信息
  def updateZkNodeData(zkNodePath:String,value:String): Unit ={
    if(!ensureZkPathExists(zkNodePath)){
      createZkNode(zkNodePath)
    }
    val stat = getCurator().setData().withVersion(-1).forPath(zkNodePath,value.getBytes())
    println("=====>修改之后的版本为：" + stat.getVersion)
  }

  //获取节点数据
  def getZkNodeData(zkPath:String): String = {
    val node10Stat = new Stat()
    val node10 = getCurator().getData()
      .storingStatIn(node10Stat) //获取stat信息存储到stat对象
      .forPath(zkPath);
    val resultData = new String(node10)
    println("=====>该节点信息为：" + resultData)
    println("=====>该节点的数据版本号为：" + node10Stat.getVersion)
    resultData
  }
  //获取节点数据
  def getZkNodeDataByWatch(): Unit = {
    val bytes = getCurator().getData().usingWatcher(new Watcher {
      override def process(watchedEvent: WatchedEvent): Unit = {
        println("=====>wathcer触发")
        println(watchedEvent)
      }
    }).forPath("/gtnode/child_1")
    println("=====>获取到的节点数据为："+new String(bytes));
  }

  //创建一个 允许所有人访问的 持久节点
  def createZkNode(zkNodePath:String): Unit = {
    getCurator().create()
      .creatingParentsIfNeeded() //递归创建,如果没有父节点,自动创建父节点
      .withMode(CreateMode.PERSISTENT) //节点类型,持久节点
      .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE) //设置ACL,和原生API相同
      //.forPath("/gtnode/child_1", "123456".getBytes()) //节点路径和节点数据
      .forPath(zkNodePath ) //节点路径
  }

  //删除节点
  def deleteZkNode(): Unit = {
    getCurator().delete().forPath("/gtnode")
  }

  //递归删除节点
  def deleteZkNodes(): Unit = {
    getCurator().delete().deletingChildrenIfNeeded().forPath("/gtnode")
  }

  // 判断zk上数据节点是否存在
  def ensureZkPathExists(zkPath: String): Boolean = {
    val existsNodeStat: Stat = getCurator().checkExists().forPath(zkPath)
    if (existsNodeStat == null) {
      println(s"${zkPath}=====>节点不存在")
      return false
    } else if (existsNodeStat.getEphemeralOwner > 0) {
      println(s"${zkPath}=====>临时节点")
      return true
    } else {
      println(s"${zkPath}=====>持久节点")
      return true
    }
  }

  // 获取Zookeeper客户端 - CuratorFramework
  def getCurator(): CuratorFramework = {
    if (curator == null) {
      val curatorFramework: CuratorFramework =
        CuratorFrameworkFactory
          .builder()
          .connectString("m0001.xh-hadoop.com:2181,m0002.xh-hadoop.com:2181,m0003.xh-hadoop.com:2181,m0004.xh-hadoop.com:2181,s0001.xh-hadoop.com:2181")
          .retryPolicy(new ExponentialBackoffRetry(1000, 3))
          //.namespace("rootPth") 设置zk的根目录,默认会在该字符串之前加上 "/"
          .build()
      curatorFramework.start()
      curator = curatorFramework
    }
    curator
  }

  //关闭
  def closeCurator(): Unit = {
    if (curator != null) {
      curator.close()
    }
  }

  //从kafka中获取topic的原始偏移量(最主要的是获取分区数等信息)
  def getEarliestOffset(topicName:String): util.Map[TopicPartition, lang.Long] ={
    val kafkaParam = new Properties
    kafkaParam.put("bootstrap.servers", "k0001.xh-kafka.com:9092,k0002.xh-kafka.com:9092,k0003.xh-kafka.com:9092,k0004.xh-kafka.com:9092,k0005.xh-kafka.com:9092")
    kafkaParam.put("group.id", "group_gt")
    kafkaParam.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaParam.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaParam.put("session.timeout.ms", "30000")
    val consumer = new KafkaConsumer(kafkaParam)
    val partitionArr = consumer.partitionsFor(topicName).toArray()
    val infoList = new util.ArrayList[TopicPartition]()
    for(partitionInfo <- partitionArr){
      val info = partitionInfo.asInstanceOf[PartitionInfo]
      infoList.add(new TopicPartition(topicName,info.partition()))
    }
    val offsets: util.Map[TopicPartition, lang.Long] = consumer.beginningOffsets(infoList)
    offsets
  }

}
