import com.google.gson.{Gson, JsonObject, JsonParser}
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}



/**
  * Created by moyong on 2017/10/27.
  */
object UserClickCountAnalytics {

  def main(args: Array[String]): Unit = {
    var masterUrl = "local[1]"
    if (args.length > 0) {
      masterUrl = args(0)
    }

    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("UserClickCountStat")
    val ssc = new StreamingContext(conf, Seconds(5))

    // Kafka configurations
    val topics = Set("user_events")
    val brokers = "127.0.0.1:9092,127.0.0.1:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")

    val dbIndex = 1
    val clickHashKey = "app::users::click"

    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val events = kafkaStream.flatMap(line => {
      val data =  new JsonParser().parse(line._2).getAsJsonObject()
      Some(data)
    })

    // Compute user click times
    val userClicks = events.map(x => (x.get("uid").getAsString, x.get("click_count").getAsInt)).reduceByKey(_ + _)
    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(pair => {
          val uid = pair._1
          val clickCount = pair._2


          val jedisPoolConfig:JedisPoolConfig  = new JedisPoolConfig();
          jedisPoolConfig.setMaxTotal(10);
          val pool:JedisPool = new JedisPool(jedisPoolConfig, "localhost", 6379);

          val jedis:Jedis = pool.getResource();

//            jedis.set("pooledJedis", "hello jedis pool!");
            jedis.hincrBy(clickHashKey, uid, clickCount)

            System.out.println(jedis.get("pooledJedis"));

            //还回pool中
            if(jedis != null){
              jedis.close()
            }


          pool.close()




//          val jedis = RedisClient.pool.getResource
//          jedis.select(dbIndex)
//          jedis.hincrBy(clickHashKey, uid, clickCount)
//          RedisClient.pool.returnResource(jedis)
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
