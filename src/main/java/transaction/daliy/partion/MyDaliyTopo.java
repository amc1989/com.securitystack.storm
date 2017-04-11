package transaction.daliy.partion;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.transactional.TransactionalTopologyBuilder;
import transcation.MyTxSpout;

public class MyDaliyTopo {

    public static void main(String[] args) {
        TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("ttbid", "spoutid", new MyPtTxSpout(), 1);
        builder.setBolt("MyDaliyBatchBolt", new MyDaliyBatchBolt(), 3).shuffleGrouping("spoutid");
        builder.setCommitterBolt("MyCommiter", new MyDaliyCommiterBolt(), 1).shuffleGrouping("MyDaliyBatchBolt");
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(3);

        if (args.length > 0) {

            try {
                // This statement   the topology on remote cluster.
                // args[0] = name of topology

                StormSubmitter.submitTopology(args[0], conf,
                        builder.buildTopology());

            } catch (Exception e) {

                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("LearningStormToplogy", conf,
                    builder.buildTopology());
            try {
                Thread.sleep(10000);
            } catch (Exception exception) {

                System.out.println("Thread interrupted exception : "
                        + exception);
            }
           /* cluster.killTopology("LearningStormToplogy");
            cluster.shutdown();*/
        }

    }

}
