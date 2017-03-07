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
package es.eucm.rage.realtime.simple.topologies;

import es.eucm.rage.realtime.filters.FieldValueFilter;
import es.eucm.rage.realtime.filters.FieldValuesOrFilter;
import es.eucm.rage.realtime.functions.*;
import es.eucm.rage.realtime.functions.DocumentBuilder;
import es.eucm.rage.realtime.states.GameplayStateUpdater;
import es.eucm.rage.realtime.states.TraceStateUpdater;

import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;

/**
 * RAGE Analytics implementation of
 * {@link es.eucm.rage.realtime.topologies.TopologyBuilder} performing the
 * real-time analysis. Check out:
 * https://github.com/e-ucm/rage-analytics/wiki/Understanding
 * -RAGE-Analytics-Traces-Flow
 */
public class TopologyBuilder implements
		es.eucm.rage.realtime.topologies.TopologyBuilder {

	@Override
	public void build(TridentTopology tridentTopology, Stream tracesStream,
			StateFactory partitionPersistFactory,
			StateFactory persistentAggregateFactory) {

		GameplayStateUpdater gameplayStateUpdater = new GameplayStateUpdater();

		/** ---> AbstractAnalysis definition <--- **/

		/*
		 * --> Analyzing for Kibana visualizations (traces index, 'sessionId')
		 * <--
		 */

		// 1 - For each TRACE_KEY (from Kibana) that we receive
		// 2 - Create an ElasticSearch "sanitized" document identified as
		// "document"
		// 3 - Finally persist the "document" to the SESSION_ID_KEY
		// ElasticSearch
		// index
		tracesStream
				.each(new Fields(TRACE_KEY), new DocumentBuilder(TRACE_KEY),
						new Fields(DOCUMENT_KEY))
				.peek(new LogConsumer("0"))
				.partitionPersist(partitionPersistFactory,
						new Fields(DOCUMENT_KEY), new TraceStateUpdater());

		/*
		 * --> Analyzing for the Alerts and Warnings system (results index,
		 * 'results-sessionId') <--
		 */

		// 1 - For each TRACE_KEY (from Kibana) that we receive
		// 2 - Extract the fields TridentTraceKeys.GAMEPLAY_ID and
		// TridentTraceKeys.EVENT so that we can play
		// with it below
		Stream gameplayIdStream = tracesStream
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(TridentTraceKeys.GAMEPLAY_ID,
								TridentTraceKeys.EVENT),
						new Fields(TridentTraceKeys.GAMEPLAY_ID,
								TridentTraceKeys.EVENT)).peek(
						new LogConsumer("1"));

		// 3 - For each TRACE_KEY (from Kibana) that we receive
		// 4 - Extract the field 'timestamp' and add it to the document per
		// 'gameplayId' (player)
		gameplayIdStream
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(TridentTraceKeys.TIMESTAMP),
						new Fields(TridentTraceKeys.TIMESTAMP))
				.each(new Fields(TridentTraceKeys.TIMESTAMP),
						new SimplePropertyCreator(TridentTraceKeys.TIMESTAMP,
								TridentTraceKeys.TIMESTAMP),
						new Fields(PROPERTY_KEY, VALUE_KEY))
				.peek(new LogConsumer("3"))
				.partitionPersist(
						partitionPersistFactory,
						new Fields(SESSION_ID_KEY,
								TridentTraceKeys.GAMEPLAY_ID, PROPERTY_KEY,
								VALUE_KEY), gameplayStateUpdater);

		// 5 - Add the name of the given player to the document ('gameplayId')
		gameplayIdStream
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(TridentTraceKeys.NAME),
						new Fields(TridentTraceKeys.NAME))
				.each(new Fields(TridentTraceKeys.NAME),
						new SimplePropertyCreator(TridentTraceKeys.NAME,
								TridentTraceKeys.NAME),
						new Fields(PROPERTY_KEY, VALUE_KEY))
				.peek(new LogConsumer("4"))
				.partitionPersist(
						partitionPersistFactory,
						new Fields(SESSION_ID_KEY,
								TridentTraceKeys.GAMEPLAY_ID, PROPERTY_KEY,
								VALUE_KEY), gameplayStateUpdater);

		// Alternatives (selected & unlocked) processing
		// 6 - For each TraceEventTypes.SELECTED or TraceEventTypes.UNLOCKED
		// trace (alternative type)
		// 7 - Count the amount of different "responses" have been made (Group
		// By TridentTraceKeys.RESPONSE)
		gameplayIdStream
				.each(new Fields(TridentTraceKeys.EVENT, TRACE_KEY),
						new FieldValuesOrFilter(TridentTraceKeys.EVENT,
								TraceEventTypes.SELECTED,
								TraceEventTypes.UNLOCKED))
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(TridentTraceKeys.TARGET,
								TridentTraceKeys.TYPE,
								TridentTraceKeys.RESPONSE),
						new Fields(TridentTraceKeys.TARGET,
								TridentTraceKeys.TYPE,
								TridentTraceKeys.RESPONSE))
				.each(new Fields(TRACE_KEY, TridentTraceKeys.TYPE,
						TridentTraceKeys.EVENT, TridentTraceKeys.TARGET,
						TridentTraceKeys.RESPONSE),
						new PropertyCreator(TRACE_KEY, TridentTraceKeys.EVENT,
								TridentTraceKeys.TYPE, TridentTraceKeys.TARGET),
						new Fields(PROPERTY_KEY, VALUE_KEY))
				.peek(new LogConsumer("5"))
				.groupBy(
						new Fields(SESSION_ID_KEY,
								TridentTraceKeys.GAMEPLAY_ID,
								TridentTraceKeys.EVENT, TridentTraceKeys.TYPE,
								TridentTraceKeys.TARGET,
								TridentTraceKeys.RESPONSE))
				.persistentAggregate(persistentAggregateFactory, new Count(),
						new Fields("count"));

		// Accessible (accessed & skipped)), GameObject (interacted & used) and
		// Completable (initialized) processing
		// 8 - For each TraceEventTypes.ACCESSED, TraceEventTypes.SKIPPED
		// (Accessible), TraceEventTypes.INITIALIZED
		// (Completable), "interacted" or TraceEventTypes.USED (GameObject)
		// trace
		// 9 - Count the amount of different "targets" have been made (Group By
		// TridentTraceKeys.TARGET)
		gameplayIdStream
				.each(new Fields(TridentTraceKeys.EVENT, TRACE_KEY),
						new FieldValuesOrFilter(TridentTraceKeys.EVENT,
								TraceEventTypes.ACCESSED,
								TraceEventTypes.SKIPPED,
								TraceEventTypes.INITIALIZED,
								TraceEventTypes.INTERACTED,
								TraceEventTypes.USED))
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(TridentTraceKeys.TARGET,
								TridentTraceKeys.TYPE),
						new Fields(TridentTraceKeys.TARGET,
								TridentTraceKeys.TYPE))
				.peek(new LogConsumer("initialized 1"))
				.each(new Fields(TRACE_KEY, TridentTraceKeys.TYPE,
						TridentTraceKeys.EVENT, TridentTraceKeys.TARGET),
						new PropertyCreator(TRACE_KEY, TridentTraceKeys.EVENT,
								TridentTraceKeys.TYPE, TridentTraceKeys.TARGET),
						new Fields(PROPERTY_KEY, VALUE_KEY))
				.peek(new LogConsumer("initialized 2"))
				.groupBy(
						new Fields(SESSION_ID_KEY,
								TridentTraceKeys.GAMEPLAY_ID,
								TridentTraceKeys.EVENT, TridentTraceKeys.TYPE,
								TridentTraceKeys.TARGET))
				.persistentAggregate(persistentAggregateFactory, new Count(),
						new Fields("count"));

		// Completable (Progressed) processing
		// 10 - For each TraceEventTypes.PROGRESSED (Completable) trace
		// 11 - Update the TridentTraceKeys.PROGRESS field to its latest value
		gameplayIdStream
				.each(new Fields(TridentTraceKeys.EVENT, TRACE_KEY),
						new FieldValueFilter(TridentTraceKeys.EVENT,
								TraceEventTypes.PROGRESSED))
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(TridentTraceKeys.TARGET,
								TridentTraceKeys.TYPE),
						new Fields(TridentTraceKeys.TARGET,
								TridentTraceKeys.TYPE))
				.each(new Fields(TRACE_KEY),
						new ExtensionsFieldExtractor(TridentTraceKeys.PROGRESS),
						new Fields(TridentTraceKeys.PROGRESS))
				.each(new Fields(TridentTraceKeys.PROGRESS,
						TridentTraceKeys.TYPE, TridentTraceKeys.EVENT,
						TridentTraceKeys.TARGET),
						new SuffixPropertyCreator(TridentTraceKeys.PROGRESS,
								TridentTraceKeys.PROGRESS,
								TridentTraceKeys.EVENT, TridentTraceKeys.TYPE,
								TridentTraceKeys.TARGET),
						new Fields(PROPERTY_KEY, VALUE_KEY))
				.peek(new LogConsumer("6"))
				.partitionPersist(
						partitionPersistFactory,
						new Fields(SESSION_ID_KEY,
								TridentTraceKeys.GAMEPLAY_ID, PROPERTY_KEY,
								VALUE_KEY), gameplayStateUpdater);

		// Completable (Completed) processing for field TridentTraceKeys.SUCCESS
		// 11 - For each TraceEventTypes.COMPLETED (Completable) trace
		// 12 - Update the TridentTraceKeys.SUCCESS field to its latest value
		gameplayIdStream
				.each(new Fields(TridentTraceKeys.EVENT, TRACE_KEY),
						new FieldValueFilter(TridentTraceKeys.EVENT,
								TraceEventTypes.COMPLETED))
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(TridentTraceKeys.TARGET,
								TridentTraceKeys.TYPE, TridentTraceKeys.SUCCESS),
						new Fields(TridentTraceKeys.TARGET,
								TridentTraceKeys.TYPE, TridentTraceKeys.SUCCESS))
				.each(new Fields(TridentTraceKeys.SUCCESS,
						TridentTraceKeys.TYPE, TridentTraceKeys.EVENT,
						TridentTraceKeys.TARGET),
						new SuffixPropertyCreator(TridentTraceKeys.SUCCESS,
								TridentTraceKeys.SUCCESS,
								TridentTraceKeys.EVENT, TridentTraceKeys.TYPE,
								TridentTraceKeys.TARGET),
						new Fields(PROPERTY_KEY, VALUE_KEY))
				.partitionPersist(
						partitionPersistFactory,
						new Fields(SESSION_ID_KEY,
								TridentTraceKeys.GAMEPLAY_ID, PROPERTY_KEY,
								VALUE_KEY), gameplayStateUpdater);

		// Completable (Completed) processing for field TridentTraceKeys.SCORE
		// 13 - For each TraceEventTypes.COMPLETED (Completable) trace
		// 14 - Update the TridentTraceKeys.SCORE field to its latest value
		gameplayIdStream
				.each(new Fields(TridentTraceKeys.EVENT, TRACE_KEY),
						new FieldValueFilter(TridentTraceKeys.EVENT,
								TraceEventTypes.COMPLETED))
				.each(new Fields(TRACE_KEY),
						new TraceFieldExtractor(TridentTraceKeys.TARGET,
								TridentTraceKeys.TYPE, TridentTraceKeys.SCORE),
						new Fields(TridentTraceKeys.TARGET,
								TridentTraceKeys.TYPE, TridentTraceKeys.SCORE))
				.each(new Fields(TridentTraceKeys.SCORE, TridentTraceKeys.TYPE,
						TridentTraceKeys.EVENT, TridentTraceKeys.TARGET),
						new SuffixPropertyCreator(TridentTraceKeys.SCORE,
								TridentTraceKeys.SCORE, TridentTraceKeys.EVENT,
								TridentTraceKeys.TYPE, TridentTraceKeys.TARGET),
						new Fields(PROPERTY_KEY, VALUE_KEY))
				.partitionPersist(
						partitionPersistFactory,
						new Fields(SESSION_ID_KEY,
								TridentTraceKeys.GAMEPLAY_ID, PROPERTY_KEY,
								VALUE_KEY), gameplayStateUpdater);

		/*
		 * --> Additional/custom analysis needed can be added here or changing
		 * the code above <--
		 */

	}

}
