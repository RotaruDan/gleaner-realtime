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
package es.eucm.rage.realtime.topologies;

import es.eucm.rage.realtime.filters.FieldValueFilter;
import es.eucm.rage.realtime.filters.FieldValuesOrFilter;
import es.eucm.rage.realtime.functions.PropertyCreator;
import es.eucm.rage.realtime.functions.SimplePropertyCreator;
import es.eucm.rage.realtime.functions.SuffixPropertyCreator;
import es.eucm.rage.realtime.functions.TraceFieldExtractor;
import es.eucm.rage.realtime.states.DocumentBuilder;
import es.eucm.rage.realtime.states.ESStateFactory;
import es.eucm.rage.realtime.states.GameplayStateUpdater;
import es.eucm.rage.realtime.states.TraceStateUpdater;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.tuple.Fields;

public class RealtimeTopology extends TridentTopology {

	public void prepareTest(ITridentSpout spout,
			ESStateFactory elasticStateFactory) {
		prepare(newStream("traces", spout), elasticStateFactory);
	}

	public void prepare(Stream traces, ESStateFactory elasticStateFactory) {

		GameplayStateUpdater gameplayStateUpdater = new GameplayStateUpdater();
		Stream tracesStream = createTracesStream(traces);

		/** ---> Analysis definition <--- **/

		tracesStream.each(
				new Fields("trace"),
				new DocumentBuilder(elasticStateFactory.getConfig()
						.getSessionId(), "trace"), new Fields("document"))
				.partitionPersist(elasticStateFactory, new Fields("document"),
						new TraceStateUpdater());

		Stream gameplayIdStream = tracesStream.each(new Fields("trace"),
				new TraceFieldExtractor("gameplayId", "event"), new Fields(
						"gameplayId", "event"));

		// Timestamp of the last trace per gameplayId
		gameplayIdStream
				.each(new Fields("trace"),
						new TraceFieldExtractor("timestamp"),
						new Fields("timestamp"))
				.each(new Fields("timestamp"),
						new SimplePropertyCreator("timestamp", "timestamp"),
						new Fields("p", "v"))
				.partitionPersist(elasticStateFactory,
						new Fields("versionId", "gameplayId", "p", "v"),
						gameplayStateUpdater);

		// Name of the given gameplayId
		gameplayIdStream
				.each(new Fields("trace"), new TraceFieldExtractor("name"),
						new Fields("name"))
				.each(new Fields("name"),
						new SimplePropertyCreator("name", "name"),
						new Fields("p", "v"))
				.partitionPersist(elasticStateFactory,
						new Fields("versionId", "gameplayId", "p", "v"),
						gameplayStateUpdater);

		// Alternatives (selected & unlocked) processing
		gameplayIdStream
				.each(new Fields("event", "trace"),
						new FieldValuesOrFilter("event", "selected", "unlocked"))
				.each(new Fields("trace"),
						new TraceFieldExtractor("target", "type", "response"),
						new Fields("target", "type", "response"))
				.each(new Fields("trace", "type", "event", "target", "response"),
						new PropertyCreator("trace", "event", "type", "target"),
						new Fields("p", "v"))
				.groupBy(
						new Fields("versionId", "gameplayId", "event", "type",
								"target", "response"))
				.persistentAggregate(elasticStateFactory, new Count(),
						new Fields("count"));

		// Accessible (accessed & skipped)), GameObject (interacted & used) and
		// Completable (initialized) processing
		gameplayIdStream
				.each(new Fields("event", "trace"),
						new FieldValuesOrFilter("event", "accessed", "skipped",
								"initialized", "interacted", "used"))
				.each(new Fields("trace"),
						new TraceFieldExtractor("target", "type"),
						new Fields("target", "type"))
				.each(new Fields("trace", "type", "event", "target"),
						new PropertyCreator("trace", "event", "type", "target"),
						new Fields("p", "v"))
				.groupBy(
						new Fields("versionId", "gameplayId", "event", "type",
								"target"))
				.persistentAggregate(elasticStateFactory, new Count(),
						new Fields("count"));

		// Completable (Progressed) processing
		gameplayIdStream
				.each(new Fields("event", "trace"),
						new FieldValueFilter("event", "progressed"))
				.each(new Fields("trace"),
						new TraceFieldExtractor("target", "type", "progress"),
						new Fields("target", "type", "progress"))
				.each(new Fields("progress", "type", "event", "target"),
						new SuffixPropertyCreator("progress", "progress",
								"event", "type", "target"),
						new Fields("p", "v"))
				.partitionPersist(elasticStateFactory,
						new Fields("versionId", "gameplayId", "p", "v"),
						gameplayStateUpdater);

		// Completable (Completed) processing for field "success"
		gameplayIdStream
				.each(new Fields("event", "trace"),
						new FieldValueFilter("event", "completed"))
				.each(new Fields("trace"),
						new TraceFieldExtractor("target", "type", "success"),
						new Fields("target", "type", "success"))
				.each(new Fields("success", "type", "event", "target"),
						new SuffixPropertyCreator("success", "success",
								"event", "type", "target"),
						new Fields("p", "v"))
				.partitionPersist(elasticStateFactory,
						new Fields("versionId", "gameplayId", "p", "v"),
						gameplayStateUpdater);

		// Completable (Completed) processing for field "score"
		gameplayIdStream
				.each(new Fields("event", "trace"),
						new FieldValueFilter("event", "completed"))
				.each(new Fields("trace"),
						new TraceFieldExtractor("target", "type", "score"),
						new Fields("target", "type", "score"))
				.each(new Fields("score", "type", "event", "target"),
						new SuffixPropertyCreator("score", "score", "event",
								"type", "target"), new Fields("p", "v"))
				.partitionPersist(elasticStateFactory,
						new Fields("versionId", "gameplayId", "p", "v"),
						gameplayStateUpdater);

	}

	protected Stream createTracesStream(Stream stream) {
		return stream;
	}
}
