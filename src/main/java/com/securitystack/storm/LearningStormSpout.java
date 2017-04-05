package com.securitystack.storm;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class LearningStormSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	@SuppressWarnings("serial")
	private static final Map<Integer, String> map = new HashMap<Integer, String>() {
		{
			put(0, "google");
			put(1, "facebook");
			put(2, "twitter");
			put(3, "youtube");
			put(4, "linkedin");
		}
	};

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void nextTuple() {
		final Random random = new Random();
		int index = random.nextInt(5);
		collector.emit(new Values(map.get(index)));
		collector.emit(new Values(map.get(index)));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("site"));
	}

}
