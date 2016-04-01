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
package es.eucm.gleaner.realtime.topologies;

import backtype.storm.tuple.Fields;
import es.eucm.gleaner.realtime.filters.FieldValueFilter;
import es.eucm.gleaner.realtime.functions.PropertyCreator;
import es.eucm.gleaner.realtime.functions.TraceFieldExtractor;
import es.eucm.gleaner.realtime.states.GameplayStateUpdater;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.spout.ITridentSpout;
import storm.trident.state.StateFactory;
import storm.trident.state.StateUpdater;

public class RealtimeTopology extends TridentTopology {

	public <T> void prepare(ITridentSpout<T> spout, StateFactory stateFactory) {
		prepare(newStream("traces", spout), stateFactory);
	}

	/**
	 * @param traces
	 *            a stream with two fields: a versionId {@link String} and a
	 *            trace {@link java.util.Map}
	 */
	public void prepare(Stream traces, StateFactory stateFactory) {

		StateUpdater stateUpdater = new GameplayStateUpdater();

		Stream eventStream = createTracesStream(traces).each(
				new Fields("trace"),
				new TraceFieldExtractor("gameplayId", "event"),
				new Fields("gameplayId", "event"));

		// Zones
		eventStream
				.each(new Fields("event", "trace"),
						new FieldValueFilter("event", "zone"))
				.each(new Fields("trace"), new TraceFieldExtractor("value"),
						new Fields("value"))
				.each(new Fields("event", "value"),
						new PropertyCreator("value", "event"),
						new Fields("p", "v"))
				.partitionPersist(stateFactory,
						new Fields("versionId", "gameplayId", "p", "v"),
						stateUpdater);

		// Variables
		eventStream
				.each(new Fields("event", "trace"),
						new FieldValueFilter("event", "var"))
				.each(new Fields("trace"),
						new TraceFieldExtractor("target", "value"),
						new Fields("var", "value"))
				.each(new Fields("event", "var", "value"),
						new PropertyCreator("value", "event", "var"),
						new Fields("p", "v"))
				.partitionPersist(stateFactory,
						new Fields("versionId", "gameplayId", "p", "v"),
						stateUpdater);

		// Choices
		eventStream
				.each(new Fields("event", "trace"),
						new FieldValueFilter("event", "choice"))
				.each(new Fields("trace"),
						new TraceFieldExtractor("target", "value"),
						new Fields("choice", "option"))
				.groupBy(
						new Fields("versionId", "gameplayId", "event",
								"choice", "option"))
				.persistentAggregate(stateFactory, new Count(),
						new Fields("count"));

		// Interactions
		eventStream
				.each(new Fields("event", "trace"),
						new FieldValueFilter("event", "interact"))
				.each(new Fields("trace"), new TraceFieldExtractor("target"),
						new Fields("target"))
				.groupBy(
						new Fields("versionId", "gameplayId", "event", "target"))
				.persistentAggregate(stateFactory, new Count(),
						new Fields("count"));
	}

	protected Stream createTracesStream(Stream stream) {
		return stream;
	}
}
