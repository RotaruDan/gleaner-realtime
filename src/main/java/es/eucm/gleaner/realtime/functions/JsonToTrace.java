package es.eucm.gleaner.realtime.functions;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class JsonToTrace implements Function {

	private String versionId;

	private JsonParser parser;

	public JsonToTrace(String versionId) {
		this.versionId = versionId;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String json = tuple.getStringByField("str");
		JsonObject rootObj = parser.parse(json).getAsJsonObject();

		JsonObject extensionsObject = rootObj.getAsJsonObject("object")
				.getAsJsonObject("definition").getAsJsonObject("extensions");

		Map<String, String> trace = new HashMap<String, String>();
		for (Map.Entry<String, JsonElement> jsonElementEntry : extensionsObject.entrySet()) {
			trace.put(jsonElementEntry.getKey(), jsonElementEntry.getValue().getAsString());
		}

		collector.emit(Arrays.asList(versionId, trace));
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		parser = new JsonParser();
	}

	@Override
	public void cleanup() {

	}
}
