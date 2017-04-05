package com.securitystack.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class LearningStormTopology {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("LearningStormSpout", new LearningStormSpout(), 1).setNumTasks(4);
		builder.setBolt("LearningStormBolt", new LearningStormBolt(), 2)
				.shuffleGrouping("LearningStormSpout");

		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(3);

		if (args.length > 0) {

			try {
				// This statement submit the topology on remote cluster.
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
