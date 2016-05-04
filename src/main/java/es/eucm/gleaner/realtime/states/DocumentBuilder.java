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
package es.eucm.gleaner.realtime.states;

import es.eucm.gleaner.realtime.utils.DBUtils;
import es.eucm.gleaner.realtime.utils.Document;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

public class DocumentBuilder implements Function {

	private final String tracesIndex;

	public DocumentBuilder(String sessionId) {
		this.tracesIndex = DBUtils.getTracesIndex(sessionId);
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {

		Map trace = (Map) tuple.getValueByField("trace");
		trace.put(ESGameplayState.STORED_KEY, new Date());

		Document<Map> doc = new Document(tracesIndex,
				ESGameplayState.RAGE_DOCUMENT_TYPE, trace, null);

		ArrayList<Object> object = new ArrayList<Object>(1);
		object.add(doc);

		collector.emit(object);
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {

	}

	@Override
	public void cleanup() {

	}
}
