package com.securitystack.storm;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class LearningStormBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	@SuppressWarnings("unused")
	private OutputCollector collector;
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector=collector;

	}

	@Override
	public void execute(Tuple input) {
		 String str  = input.getString(0);
		 System.err.println("Name of input site is : "+str);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 

	}

}
