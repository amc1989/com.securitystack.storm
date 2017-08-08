package kafka.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;

import java.util.UUID;

/**
 * @FileName: MyTopology.java
 * @Description: MyTopology.java类说明
 * @Author: zhulei
 * @Date: 2017/8/8 上午10:56
 * @Copyright: 2017 dingxiang-inc.com Inc. All rights reserved.
 */

public class MyTopology {

    private static String topicName = "mykafka";
    private static String zkRoot = "/stormKafka/"+topicName;
    public static  void main(String [] args){


        BrokerHosts hosts = new ZkHosts("127.0.0.1:2181");
        SpoutConfig spoutConfig = new SpoutConfig(hosts,topicName,zkRoot, UUID.randomUUID().toString());

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout",kafkaSpout);
        builder.setBolt("merchantsSalesBolt", new KafkaBolt(), 2).shuffleGrouping("kafkaSpout");

        Config conf = new Config();
        conf.setDebug(true);

        if(args != null && args.length > 0) {
            conf.setNumWorkers(1);

            try {
                StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }


        } else {

            conf.setMaxSpoutPending(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("ordersAnalysis", conf, builder.createTopology());


        }

    }
}

