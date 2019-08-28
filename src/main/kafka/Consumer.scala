import java.util
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}

object Consumer {
  def main(args: Array[String]): Unit = {

    val prop=new Properties()

    prop.put("bootstrap.servers","hadoop01:9092,hadoop02:9092,hadoop03:9092")
    prop.put("group.id","group01")
    prop.put("auto.offset.reset","earliest")
    prop.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    val consumer=new KafkaConsumer[String,String](prop)
    consumer.subscribe(Collections.singletonList("test"))
    while(true){
      val msgs: ConsumerRecords[String, String] = consumer.poll(1000)

      val it: util.Iterator[ConsumerRecord[String, String]] = msgs.iterator()

      while(it.hasNext){
        val msg=it.next()

        println(s"${msg.value()}")
      }
    }

  }
}
