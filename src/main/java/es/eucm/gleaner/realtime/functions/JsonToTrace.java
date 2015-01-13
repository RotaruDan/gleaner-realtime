package es.eucm.gleaner.realtime.functions;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;

public class JsonToTrace implements Function {

	private String versionId;

	private Gson gson;

	private Type type;

	public JsonToTrace(String versionId) {
		this.versionId = versionId;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Object trace = gson.fromJson(tuple.getStringByField("str"), type);
		collector.emit(Arrays.asList(versionId, trace));
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		gson = new Gson();
		type = new TypeToken<Map<String, Object>>() {
		}.getType();
	}

	@Override
	public void cleanup() {

	}
}
