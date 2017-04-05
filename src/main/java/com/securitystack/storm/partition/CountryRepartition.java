package com.securitystack.storm.partition;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.shade.com.google.common.collect.ImmutableMap;
import org.apache.storm.task.WorkerTopologyContext;

public class CountryRepartition implements CustomStreamGrouping, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Map<String, Integer> countries = ImmutableMap.of(
			"India", 0, "Japan", 1, "United State", 2, "China", 3, "Brazil", 4);
	private int tasks = 0;

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		tasks = targetTasks.size();

	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		String country = (String) values.get(0);
		return ImmutableList.of(countries.get(country) % tasks);
	}

}
