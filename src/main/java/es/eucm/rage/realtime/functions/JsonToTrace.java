/**
 * Copyright (C) 2016 e-UCM (http://www.e-ucm.es/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.eucm.rage.realtime.functions;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;

public class JsonToTrace implements Function {

	private String versionId;

	private Gson gson;

	private Type type;

	/**
	 * Given a JSON Trace ("str" key, from Kafka) string returns a "trace" ->
	 * Map<String, Object>
	 * 
	 * @param versionId
	 */
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
