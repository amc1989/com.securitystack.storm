package transcation;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.transactional.TransactionalTopologyBuilder;
import org.apache.storm.tuple.Fields;
import user_visit.DeepVisit;
import user_visit.FmtLogBolt;
import user_visit.UVSumBolt;
import visit.SourceSpout;

public class MyTopo {

    public static void main(String[] args) {
        TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("ttbid", "spoutid", new MyTxSpout(), 1);
        builder.setBolt("MyTransactionBolt", new MyTransactionBolt(), 3).shuffleGrouping("spoutid");
        builder.setCommitterBolt("MyCommiter", new MyCommiter(), 1).shuffleGrouping("MyTransactionBolt");
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
