package es.eucm.gleaner.realtime.functions;

import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Arrays;
import java.util.Map;

public class PropertyCreator implements Function {

	private String valueField;

	private String[] keysField;

	public PropertyCreator(String valueField, String... keysField) {
		this.valueField = valueField;
		this.keysField = keysField;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		collector.emit(Arrays.asList(toPropertyKey(tuple),tuple.getValueByField(valueField)));
	}

	public String toPropertyKey(TridentTuple tuple) {
		String result = "";
		for (String key : keysField) {
			result += tuple.getStringByField(key) + ".";
		}
		return result.substring(0, result.length() - 1);
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
	}
}
