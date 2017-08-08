package kafka.single;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @FileName: KfkaProducer.java
 * @Description: KfkaProducer.java类说明
 * @Author: zhulei
 * @Date: 2017/8/7 上午10:57
 * @Copyright: 2017 dingxiang-inc.com Inc. All rights reserved.
 */

public class KfkaProducer {
    public static void main(String[] args) {


        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer kafkaProducer = new KafkaProducer(props);

        for(int i=2000;i<3000;i++) {
            System.err.println("zhulei_test"+i);
            ProducerRecord<String, String> producerRecord = new ProducerRecord("mykafka", "aa", "zhulei_test"+i);
            kafkaProducer.send(producerRecord);
        }



    }
}
