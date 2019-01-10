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
package es.eucm.rage.realtime.topologies;

import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.state.StateFactory;

import java.util.Map;

/**
 * Configures a {@link org.apache.storm.trident.TridentTopology} when invoking
 * the method
 * {@link TopologyBuilder#build(TridentTopology, Stream, StateFactory, StateFactory)}
 * .
 */
public interface TopologyBuilder {

	/**
	 * TODO ..
	 */
	String OUT_KEY = "out";
	/**
	 * Used to wrap {@link es.eucm.rage.realtime.utils.Document} inside the
	 * {@link org.apache.storm.trident.TridentTopology} to persist inside an
	 * ElasticSearch index
	 */
	String DOCUMENT_KEY = "document";
	/**
	 * Provided from the Flux configuration, used to know to which session the
	 * trace belongs. See:
	 * https://github.com/e-ucm/rage-analytics/wiki/Understanding
	 * -RAGE-Analytics-Traces-Flow#storm-flux-configuration-files
	 */
	String ACTIVITY_ID_KEY = "activityId";
	/**
	 * Provided from the Flux configuration, used to know to which session the
	 * trace belongs. See:
	 * https://github.com/e-ucm/rage-analytics/wiki/Understanding
	 * -RAGE-Analytics-Traces-Flow#storm-flux-configuration-files
	 */
	String ACTIVITY_NAME_KEY = "activityName";
	/**
	 * The ID this trace comes from, if not available, is a leaf
	 */
	String CHILD_ACTIVITY_ID_KEY = "childActivityId";
	/**
	 * The ID this array of id (strings) that are children
	 */
	String CHILDREN = "children";
	/**
	 * The ID the "weights" array in the "weights_analyticsId" analytics object
	 */
	String WEIGHTS = "weights";
	/**
	 * The KEY of the "weights[i].name" string
	 */
	String OPERATION_NAME_KEY = "name";
	/**
	 * The KEY of the "weights[i].op" string
	 */
	String OPERATION_KEY = "op";
	/**
	 * The KEY of the "weights[i].children[j].id" string
	 */
	String OPERATION_CHILD_ID_KEY = "id";
	/**
	 * The KEY of the "weights[i].children[j].multiplier" string
	 */
	String OPERATION_CHILD_MULTIPLIER_KEY = "multiplier";
	/**
	 * The KEY of the "weights[i].children[j].needsUpdate" string
	 */
	String OPERATION_CHILDREN_NEEDSUPDATE_KEY = "needsUpdate";
	/**
	 * The KEY of the "weights[i].children[j].value" string
	 */
	String OPERATION_CHILDREN_VALUE_KEY = "value";
	/**
	 * The KEY of the "weights[i].children" array
	 */
	String OPERATION_CHILDREN_KEY = "children";
	/**
	 * The ID this trace comes from, if not available, is a leaf
	 */
	String ORIGINAL_ID = "orginalId";
	/**
	 * Used to know to which GLP the trace belongs to.
	 */
	String GLP_ID_KEY = "glpId";
	String ANALYTICS_KEY = "analytics";
	String ROOT_ANALYTICS_KEY = "rootAnalytics";
	String ANALYTICS_PARENT_ID_KEY = "parentId";

	String UUIDV4 = "uuidv4";
	/**
	 * Identifies the trace object {@link java.util.Map}
	 */
	String TRACE_KEY = "trace";
	/**
	 * Identifies the "ext" extensions object {@link java.util.Map} inside a
	 * Trace
	 */
	String EXTENSIONS_KEY = "ext";
	/**
	 * Used by the {@link es.eucm.rage.realtime.states.GameplayStateUpdater} to
	 * set up the correct property inside the game-play state
	 */
	String PROPERTY_KEY = "p";
	/**
	 * Used by the {@link es.eucm.rage.realtime.states.GameplayStateUpdater} to
	 * set up the correct value inside the game-play state
	 */
	String VALUE_KEY = "v";
	/**
	 * Analytics Name
	 */
	String ANALYTICS_NAME = "name";
	/**
	 * Analytics Name
	 */
	String TRACE_ANALYTICS_ORIGINAL_NAME = "originalName";

	/**
	 * TODO ..
	 */
	String GAMEPLAY_ID = "gameplayId";
	/**
	 * TODO ..
	 */
	String CLASS_ID = "classId";

	/**
	 * Field keys available inside the trace object. Check out the detailed
	 * description of the trace fields:
	 * https://github.com/e-ucm/rage-analytics/wiki
	 * /Understanding-RAGE-Analytics-Traces-Flow#step-2---collector-to-realtime
	 */
	interface TridentTraceKeys {
		String EVENT = "event";
		String TIMESTAMP = "timestamp";
		String NAME = "name";
		String TARGET = "target";
		String TYPE = "type";
		String RESPONSE = "response";
		String PROGRESS = "progress";
		String SUCCESS = "success";
		String SCORE = "score";
		String STORED = "stored";
		String TIME = "time";
	}

	/**
	 * Different type of values the
	 * {@link TopologyBuilder.TridentTraceKeys#EVENT} can have. Directly mapped
	 * from the Statement.Verb.Id (AxAPI-seriousgames profile), see:
	 * https://github
	 * .com/e-ucm/rage-analytics/wiki/Understanding-RAGE-Analytics-
	 * Traces-Flow#step-1---tracker-to-collector
	 */
	interface TraceEventTypes {
		String SELECTED = "selected";
		String UNLOCKED = "unlocked";
		String ACCESSED = "accessed";
		String SKIPPED = "skipped";
		String INITIALIZED = "initialized";
		String INTERACTED = "interacted";
		String USED = "used";
		String PROGRESSED = "progressed";
		String COMPLETED = "completed";
	}

	/**
	 * Builds an storm topology.
	 * 
	 * @param tridentTopology
	 * @param tracesStream
	 *            Incoming traces {@link Stream}.
	 * 
	 */
	void build(TridentTopology tridentTopology, OpaqueTridentKafkaSpout spout,
			Stream tracesStream, StateFactory partitionPersistFactory,
			StateFactory persistentAggregateFactory, Map<String, Object> conf);
}
