package es.eucm.gleaner.realtime.functions;

import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.Map;

public class TraceFieldExtractor implements Function {

	private String[] fields;

	public TraceFieldExtractor(String... fields) {
		this.fields = fields;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Map trace = (Map) tuple.getValueByField("trace");
		ArrayList<Object> object = new ArrayList<Object>();
		for (String field : fields) {
			object.add(trace.get(field));
		}
		collector.emit(object);
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {

        System.out.println();
	}

	@Override
	public void cleanup() {

	}
}
