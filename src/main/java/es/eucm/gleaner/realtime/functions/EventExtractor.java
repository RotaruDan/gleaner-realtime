package es.eucm.gleaner.realtime.functions;

import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Arrays;
import java.util.Map;

public class EventExtractor implements Function {
	@Override
	public void execute(TridentTuple objects, TridentCollector tridentCollector) {
		Map trace = (Map) objects.getValueByField("trace");
		Object event = trace.get("event");
		if (event != null) {
			tridentCollector.emit(Arrays.asList(event));
		}
	}

	@Override
	public void prepare(Map map, TridentOperationContext tridentOperationContext) {

	}

	@Override
	public void cleanup() {

	}
}
