
package trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.FirstN;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import trident.function.MySplit;
import trident.function.Split;

import java.util.Random;


public class TridentPVTopo {


    public static StormTopology buildTopology(LocalDRPC drpc) {
        Random random = new Random();
        String[] hosts = {"www.taobao.com"};
        String[] session_id = {"1", "2", "3", "4", "5", "6"};
        String[] time = {"2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-08 08:40:52",
                "2014-01-07 09:40:49", "2014-01-07 10:40:49", "2014-01-08 11:40:49", "2014-01-09 12:40:49"};


        FixedBatchSpout spout = new FixedBatchSpout(
                new Fields("eachlog"), 3,
                new Values(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(7)]),
                new Values(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(7)]),
                new Values(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(7)]),
                new Values(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(7)]),
                new Values(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(7)]),
                new Values(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(7)]),
                new Values(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(7)]),
                new Values(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(7)]),
                new Values(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(7)]),
                new Values(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(7)]),
                new Values(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(7)]),
                new Values(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(7)]));

        spout.setCycle(false);


        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology.newStream("spout1", spout)
//                .parallelismHint(16)
                .each(new Fields("eachlog"), new MySplit("\t"), new Fields("date","session_id"))
                .groupBy(new Fields("date"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("session_id"),new Count(), new Fields("PV"));
//                .parallelismHint(16);

        topology.newDRPCStream("GetPV", drpc)
                //args固定的不能换其他名称
                .each(new Fields("args"), new Split(), new Fields("date"))
                .groupBy(new Fields("date"))
                .stateQuery(wordCounts, new Fields("date"), new MapGet(), new Fields("PV"))
                .each(new Fields("PV"), new FilterNull())
                .applyAssembly(new FirstN(3,"PV",true));
//        .aggregate(new Fields("count"));
//                .project(new Fields("word", "count"));
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(10);
        conf.setDebug(false);
        if (args.length == 0) {
            try {
                LocalDRPC drpc = new LocalDRPC();
                LocalCluster cluster = new LocalCluster();

                cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
                {
                    for (int i = 0; i < 100; i++) {
                        System.err.println("DRPC RESULT: " + drpc.execute("GetPV", "2014-01-07 2014-01-08 2014-01-09"));
                        Thread.sleep(1000);
                    }
                }
            } catch (Exception e) {
            }
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, buildTopology(null));
        }
    }
}
