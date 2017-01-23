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

		/*
		 * --> Analyzing for Kibana visualizations (traces index, 'sessionId')
		 * <--
		 */

		// 1 - For each "trace" (from Kibana) that we receive
		// 2 - Create an ElasticSearch "sanitized" document identified as
		// "document"
		// 3 - Finally persist the "document" to the "sessionId" ElasticSearch
		// index
		tracesStream.each(
				new Fields("trace"),
				new DocumentBuilder(elasticStateFactory.getConfig()
						.getSessionId(), "trace"), new Fields("document"))
				.partitionPersist(elasticStateFactory, new Fields("document"),
						new TraceStateUpdater());

		/*
		 * --> Analyzing for the Alerts and Warnings system (results index,
		 * 'results-sessionId') <--
		 */

		// 1 - For each "trace" (from Kibana) that we receive
		// 2 - Extract the fields "gameplayId" and "event" so that we can play
		// with it below
		Stream gameplayIdStream = tracesStream.each(new Fields("trace"),
				new TraceFieldExtractor("gameplayId", "event"), new Fields(
						"gameplayId", "event"));

		// 3 - For each "trace" (from Kibana) that we receive
		// 4 - Extract the field 'timestamp' and add it to the document per
		// 'gameplayId' (player)
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

		// 5 - Add the name of the given player to the document ('gameplayId')
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
		// 6 - For each "selected" or "unlocked" trace (alternative type)
		// 7 - Count the amount of different "responses" have been made (Group
		// By "response")
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
		// 8 - For each "accessed", "skipped" (Accessible), "initialized"
		// (Completable), "interacted or "used" (GameObject) trace
		// 9 - Count the amount of different "targets" have been made (Group By
		// "target")
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
		// 10 - For each "progressed" (Completable) trace
		// 11 - Update the "progress" field to its latest value
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
		// 10 - For each "progressed" (Completable) trace
		// 11 - Update the "success" field to its latest value
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
		// 10 - For each "progressed" (Completable) trace
		// 11 - Update the "score" field to its latest value
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

		/*
		 * --> Additional/custom analysis needed can be added here or changing
		 * the code above <--
		 */

	}

	protected Stream createTracesStream(Stream stream) {
		return stream;
	}
}
