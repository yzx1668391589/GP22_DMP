import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Producer { def main(args: Array[String]): Unit = {

  val prop=new Properties()

  prop.put("bootstrap.servers","hadoop01:9092,hadoop02:9092,hadoop03:9092")
  prop.put("acks","0")
  prop.put("retries","3")
  prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
  prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

  val producer=new KafkaProducer[String,String](prop)

  for(i<-1 to 10000)
  {
    val msg=s"$i :this is kafaka data"
    producer.send(new ProducerRecord[String,String]("test",msg))

    Thread.sleep(500)
  }
  producer.close()

}

}
