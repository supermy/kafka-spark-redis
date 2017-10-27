import java.util  
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
  * kafka 数据生产
  *  (1)KafkaWordCountProducer
    选择KafkaWordCount.scala中的KafkaWordCountProducer方法
    VM options 设置为:-Dspark.master=local
    设置程序输入参数,Program arguments: localhost:9092 test 3 5
  *
  */
object KafkaWordCountProducer {  
  def main(args: Array[String]) {  
//    val topic = "test"
//    val brokers = "localhost:9092"
//    val messagesPerSec= 3 //每秒发送几条信息
//    val wordsPerMessage = 5 //一条信息包括多少个单词

    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args

    // Zookeeper connection properties  配置 zookeeper 参数
    val props = new util.HashMap[String, Object]()  
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)  
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,  
      "org.apache.kafka.common.serialization.StringSerializer")  
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,  
      "org.apache.kafka.common.serialization.StringSerializer")  
      
    val producer = new KafkaProducer[String, String](props)  
    // Send some messages  
    while(true) {  
      (0 to messagesPerSec.toInt).foreach { messageNum =>
        //参数5个随机数，用空格分开
        val str = (0 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt().toString).mkString(" ")
        //topic 与 随机数 构造消息
        val message = new ProducerRecord[String, String](topic, null, str)
        //发送消息到 kafka
        producer.send(message)
        //打印消息
        println(message)
      }
      Thread.sleep(10)

    }  
  }  
}  