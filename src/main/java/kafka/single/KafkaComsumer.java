package kafka.single;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @FileName: KafkaComsumer.java
 * @Description: KafkaComsumer.java类说明
 * @Author: zhulei
 * @Date: 2017/8/7 下午4:59
 * @Copyright: 2017 dingxiang-inc.com Inc. All rights reserved.
 */

public class KafkaComsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");

        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test");
        KafkaConsumer kafkaConsumer = new KafkaConsumer(props);
        kafkaConsumer.subscribe(new ArrayList<String>() {
            {
                add("mykafka");
            }


        });
        while(true){
            ConsumerRecords<String, String> records=kafkaConsumer.poll(1000);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
        }





    }
}
