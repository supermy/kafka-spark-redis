import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

/**
  * 消费数据
  *
  * <!-- 注意:这里scala的版本要和自己电脑上安装的scala一致，而且要注意spark对应的版本是否支持scala对应的版本。否则会存在版本冲突问题 -->
  * 步骤2：运行KafkaWordCount
  * args：localhost:2181 test-consumer-group test 1
  * localhost:2181表示zookeeper的监听地址，test-consumer-group表示consumer-group的名称，test表示topic，1表示线程数。
  *
  *
  * 基于direct的方式，使用kafka的简单api，Spark Streaming自己就负责追踪消费的offset，并保存在checkpoint中。Spark自己一定是同步的，因此可以保证数据是消费一次且仅消费一次。
  * 1. val topics = Set("teststreaming")
2. val brokers = "bdc46.hexun.com:9092,bdc53.hexun.com:9092,bdc54.hexun.com:9092"
3. val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")
4. // Create a direct stream
5. val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
6. val events = kafkaStream.flatMap(line => {
7. Some(line.toString())
8. })
  新api的Direct应该是以后的首选方式
  *
  * Created by moyong on 2017/10/27.
  */
object KafkaWordCount {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount    ")
      System.exit(1)
    }


    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")  //spark 配置
    val ssc =  new StreamingContext(sparkConf, Seconds(2)) //spark 流配置
    ssc.checkpoint("checkpoint") //配置检查点

    val topicpMap = topics.split(",").map((_,numThreads.toInt)).toMap //获取 topic 列表
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicpMap).map(_._2) //创建流并且获取数据
    val words = lines.flatMap(_.split(" "))  //拆分数据
    val wordCounts = words.map(x => (x, 1L))  //构造 map 值为 key,数据为1
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2) //进行窗口统计，按值排序
//      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(10), Seconds(2), 2) //进行窗口统计，按值排序

    wordCounts.print() //打印统计数据

    ssc.start()
    ssc.awaitTermination()
  }
}