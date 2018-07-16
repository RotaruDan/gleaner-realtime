/**
 * Copyright © 2016 e-UCM (http://www.e-ucm.es/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.eucm.rage.realtime.functions;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class JsonToTrace extends BaseFunction {

	/**
	 * @see java.io.Serializable
	 */
	private static final long serialVersionUID = 6729521063923226601L;

	private static final Logger LOGGER = LoggerFactory.getLogger(JsonToTrace.class.getName());
	
	private Gson gson;

	private Type type;
	
	private String field;

	/**
	 * Given a JSON Trace in {@link JsonToTrace#field}}, from
	 * Kafka) string returns a
	 * {@link es.eucm.rage.realtime.topologies.TopologyBuilder#TRACE_KEY} ->
	 * Map<String, Object>
	 * 
	 */
	public JsonToTrace(String field) {
		this.field = field;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		try {
			String jsonStr = tuple.getStringByField(this.field);
			Object trace = gson.fromJson(
					jsonStr,
					this.type);
			collector.emit(Arrays.asList(trace));
		} catch (Exception ex) {
			LOGGER.error("Error parsing string to JSON (data is lost!)", ex);
		}
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TridentOperationContext context) {
		this.gson = new Gson();
		this.type = new TypeToken<Map<String, Object>>() {}.getType();
	}
}
