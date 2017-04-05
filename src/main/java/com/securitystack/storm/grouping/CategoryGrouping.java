package com.securitystack.storm.grouping;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.shade.com.google.common.collect.ImmutableMap;
import org.apache.storm.task.WorkerTopologyContext;

public class CategoryGrouping implements CustomStreamGrouping,Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	// Mapping of category to integer values for grouping
	private static final Map<String, Integer> categories =
			ImmutableMap.of
			(
			"Financial", 0,
			"Medical", 1,
			"FMCG", 2,
			"Electronics", 3
			);
	
	// number of tasks, this is initialized in prepare method
	private int tasks = 0;
	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		// initialize the number of tasks
		
		tasks = targetTasks.size();
		
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		// return the taskId for a given category
		String category = (String) values.get(0);
		
		return ImmutableList.of(categories.get(category) % tasks);
	}

}
