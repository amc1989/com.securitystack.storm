package user_visit;

import com.securitystack.storm.LearningStormBolt;
import com.securitystack.storm.LearningStormSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import visit.SourceSpout;

public class UVTopo {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new SourceSpout(), 1) ;
		builder.setBolt("FmtLogBolt", new FmtLogBolt(), 2).setNumTasks(4).shuffleGrouping("spout");
		builder.setBolt("DeepVisit", new DeepVisit(), 4).fieldsGrouping("FmtLogBolt",new Fields("date","session_id"));
		builder.setBolt("UVSumBolt", new UVSumBolt(), 1).shuffleGrouping("DeepVisit");


		Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(3);

		if (args.length > 0) {

			try {
				// This statement   the topology on remote cluster.
				// args[0] = name of topology

				StormSubmitter.submitTopology(args[0], conf,
						builder.createTopology());
			} catch (AlreadyAliveException alreadyAliveException) {

				System.out.println(alreadyAliveException);

			} catch (InvalidTopologyException invalidTopologyException) {

				System.out.println(invalidTopologyException);

			} catch (AuthorizationException e) {

				e.printStackTrace();
			}
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("LearningStormToplogy", conf,
					builder.createTopology());
			try {
				Thread.sleep(10000);
			} catch (Exception exception) {

				System.out.println("Thread interrupted exception : "
						+ exception);
			}
			cluster.killTopology("LearningStormToplogy");
			cluster.shutdown();
		}

	}

}
