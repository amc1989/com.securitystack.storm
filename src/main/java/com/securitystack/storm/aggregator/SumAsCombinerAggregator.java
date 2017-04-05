package com.securitystack.storm.aggregator;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

import clojure.lang.Numbers;

public class SumAsCombinerAggregator implements CombinerAggregator<Number> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Number init(TridentTuple tridentTuple) {
		return (Number) tridentTuple.getValue(0);
	}

	@Override
	public Number combine(Number number1, Number number2) {
		return Numbers.add(number1, number2);
	}

	@Override
	public Number zero() {
		return 0;
	}

}
