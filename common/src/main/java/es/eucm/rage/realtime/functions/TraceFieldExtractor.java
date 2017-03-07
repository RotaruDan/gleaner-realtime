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

import es.eucm.rage.realtime.topologies.TopologyBuilder;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.Map;

public class TraceFieldExtractor implements Function {

	private String[] fields;

	/**
	 * Extracts fields from a "trace" touple
	 * 
	 * @param fields
	 *            used to extracts objects from the {@link TridentTuple} and
	 *            emit them preserving the order.
	 */
	public TraceFieldExtractor(String... fields) {
		this.fields = fields;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Map trace = (Map) tuple.getValueByField(TopologyBuilder.TRACE_KEY);
		ArrayList<Object> object = new ArrayList<Object>();
		for (String field : fields) {
			object.add(trace.get(field));
		}
		collector.emit(object);
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {

	}

	@Override
	public void cleanup() {

	}
}