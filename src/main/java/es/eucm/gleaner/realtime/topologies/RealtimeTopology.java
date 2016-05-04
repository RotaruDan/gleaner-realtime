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
import es.eucm.gleaner.realtime.utils.EsConfig;
import es.eucm.gleaner.realtime.states.DocumentBuilder;
import es.eucm.gleaner.realtime.filters.FieldValueFilter;
import es.eucm.gleaner.realtime.functions.PropertyCreator;
import es.eucm.gleaner.realtime.functions.TraceFieldExtractor;
import es.eucm.gleaner.realtime.states.ESStateFactory;
import es.eucm.gleaner.realtime.states.GameplayStateUpdater;
import es.eucm.gleaner.realtime.states.TraceStateUpdater;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.fluent.GroupedStream;
import storm.trident.operation.builtin.Count;
import storm.trident.spout.ITridentSpout;
import storm.trident.state.StateFactory;

public class RealtimeTopology extends TridentTopology {

	public <T> void prepareTest(ITridentSpout<T> spout,
			StateFactory stateFactory, EsConfig esConfig) {
		prepare(newStream("traces", spout), stateFactory, esConfig);
	}

	public void prepare(Stream traces, StateFactory stateFactory,
			EsConfig esConfig) {

		GameplayStateUpdater gameplayStateUpdater = new GameplayStateUpdater();

		Stream tracesStream = createTracesStream(traces);

		/** ---> Analysis definition <--- **/

		Stream eventStream = tracesStream.each(new Fields("trace"),
				new TraceFieldExtractor("gameplayId", "event"), new Fields(
						"gameplayId", "event"));

		// Zones
		Stream zonesTridentStream = eventStream
				.each(new Fields("event", "trace"),
						new FieldValueFilter("event", "zone"))
				.each(new Fields("trace"), new TraceFieldExtractor("value"),
						new Fields("value"))
				.each(new Fields("event", "value"),
						new PropertyCreator("value", "event"),
						new Fields("p", "v"));

		// Variables
		Stream variablesTridentStream = eventStream
				.each(new Fields("event", "trace"),
						new FieldValueFilter("event", "var"))
				.each(new Fields("trace"),
						new TraceFieldExtractor("target", "value"),
						new Fields("var", "value"))
				.each(new Fields("event", "var", "value"),
						new PropertyCreator("value", "event", "var"),
						new Fields("p", "v"));

		// Choices
		GroupedStream choicesTridentStream = eventStream
				.each(new Fields("event", "trace"),
						new FieldValueFilter("event", "choice"))
				.each(new Fields("trace"),
						new TraceFieldExtractor("target", "value"),
						new Fields("choice", "option"))
				.groupBy(
						new Fields("versionId", "gameplayId", "event",
								"choice", "option"));

		// Interactions
		GroupedStream interactionsTridentStream = eventStream
				.each(new Fields("event", "trace"),
						new FieldValueFilter("event", "interact"))
				.each(new Fields("trace"), new TraceFieldExtractor("target"),
						new Fields("target"))
				.groupBy(
						new Fields("versionId", "gameplayId", "event", "target"));

		/** ---> Results Persistance: Mongo DB & ElasticSearch <--- **/

		// Output the GameplayState to Mongo DB

		// Mongo DB Zone Persist
		zonesTridentStream.partitionPersist(stateFactory, new Fields(
				"versionId", "gameplayId", "p", "v"), gameplayStateUpdater);

		// MongoDB Variables Persist
		variablesTridentStream.partitionPersist(stateFactory, new Fields(
				"versionId", "gameplayId", "p", "v"), gameplayStateUpdater);

		choicesTridentStream.persistentAggregate(stateFactory, new Count(),
				new Fields("count"));

		interactionsTridentStream.persistentAggregate(stateFactory,
				new Count(), new Fields("count"));

		// Elastic Search Output
		if (esConfig != null) {

			// Send the received traces directly to
			ESStateFactory factory = new ESStateFactory(esConfig);
			tracesStream.each(new Fields("trace"),
					new DocumentBuilder(esConfig.getSessionId()),
					new Fields("document")).partitionPersist(factory,
					new Fields("document"), new TraceStateUpdater());

			// Output the GameplayState to Elasticsearch

			// Zones ES Persist
			zonesTridentStream.partitionPersist(factory, new Fields(
					"versionId", "gameplayId", "p", "v"), gameplayStateUpdater);

			// Variables ES Persist
			variablesTridentStream.partitionPersist(factory, new Fields(
					"versionId", "gameplayId", "p", "v"), gameplayStateUpdater);

			// Choices ES Persist
			choicesTridentStream.persistentAggregate(factory, new Count(),
					new Fields("count"));

			// Interactions ES Persist
			interactionsTridentStream.persistentAggregate(factory, new Count(),
					new Fields("count"));
		}
	}

	protected Stream createTracesStream(Stream stream) {
		return stream;
	}
}
