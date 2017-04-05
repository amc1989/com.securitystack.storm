package com.securitystack.storm.aggregator;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

public class SumAsAggregator extends BaseAggregator<SumAsAggregator.State> {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	static class State{
		long count;
	}

	private ITuple tridentTuple;

	@Override
	// Initialize the state
	public State init(Object batchId, TridentCollector collector) {
		return new State();
	}

	@Override
	// Maintain the state of sum into count variable.
	public void aggregate(State state, TridentTuple tuple,
			TridentCollector collector) {
		state.count = tridentTuple.getLong(0) + state.count;
		
	}

	@Override
	// return a tuple with single value as output
	// after processing all the tuples of given batch.
	public void complete(State state, TridentCollector tridentCollector) {
		tridentCollector.emit(new Values(state.count));
		
	}

	

}
