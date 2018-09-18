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
package es.eucm.rage.realtime;

import es.eucm.rage.realtime.functions.JsonToTrace;
import es.eucm.rage.realtime.topologies.KafkaSpoutBuilder;
import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.utils.ESUtils;
import es.eucm.rage.realtime.states.elasticsearch.EsMapState;
import es.eucm.rage.realtime.states.elasticsearch.EsState;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Base class for RAGE realtime analysis.
 */
public abstract class AbstractAnalysis {

	public static final String ELASTICSEARCH_URL_FLUX_PARAM = "elasticsearchUrl";
	public static final String ZOOKEEPER_URL_FLUX_PARAM = "zookeeperUrl";
	public static final String TOPIC_NAME_FLUX_PARAM = "topicName";
	private static final String NOW_DATE = String.valueOf(
			(new SimpleDateFormat("dd_MM_yyyy_HH_mm_ss").format(new Date())))
			.toLowerCase();
	public static final String INPUT_SPOUT_TX_ID = "input_" + NOW_DATE;

	/**
	 * Storm Flux Start-up function
	 * 
	 * @param conf
	 *            Map object with the 'flux.yml' contents (contains sessionId,
	 *            zookeeperUrl, elasticsearchUrl)
	 * @see <a
	 *      href="https://github.com/apache/storm/tree/master/external/flux#existing-topologies">Creating
	 *      a Storm topology</a>
	 */
	public StormTopology getTopology(Map<String, Object> conf) {
		/*
		 * Note that 'conf' object contains the Storm Flux configuration
		 * parameters (defined in the 'flux.yml' file). As a Java Map, the
		 * values can easily be accessed. For instance:
		 * 
		 * String sessionId = conf.get("sessionId").toString(); String
		 * zookeeperUrl = conf.get("zookeeperUrl").toString(); String
		 * elasticsearchUrl= conf.get("elasticsearchUrl").toString();
		 * 
		 * For more information check out: Storm Flux, 'Existing Topologies'
		 * documentation:
		 * https://github.com/apache/storm/tree/master/external/flux
		 * #existing-topologies RAGE Analytics Backend 'flux.yml' format:
		 * https:/
		 * /github.com/e-ucm/rage-analytics-backend/blob/master/default-flux.yml
		 */
		// Metrics for EsMapState
		conf.put(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 60);
		String topicName = conf.get(TOPIC_NAME_FLUX_PARAM).toString();
		String zookeeperUrl = conf.get(ZOOKEEPER_URL_FLUX_PARAM).toString();
		return buildTopology(topicName, zookeeperUrl, conf);
	}

	/**
	 * Builds a KafkaTopology
	 * 
	 * @param topicName
	 *            Used for the creation of ElasticSearch indices:
	 *            {@link ESUtils#getTracesIndex(String)} and
	 *            {@link ESUtils#getResultsIndex(String)}
	 * @param zookeeperUrl
	 *            Used to connect to Kafka and pull data from the topic
	 *            'sessionId'
	 * @return a topology that connects to kafka and performs the realtime
	 *         analysis
	 */
	private StormTopology buildTopology(String topicName, String zookeeperUrl,
			Map<String, Object> conf) {

		// Create a connection to ES
		StateFactory persistentAggregateFactory = EsMapState.opaque();
		StateFactory partitionPersistFactory = EsState.opaque();

		// Create the transactional kafka spout
		OpaqueTridentKafkaSpout spout = new KafkaSpoutBuilder()
				.zookeeper(zookeeperUrl).topic(topicName).build();

		// Create a base topology
		TridentTopology tridentTopology = new TridentTopology();

		// Create and enhance the input kafka stream
		Stream stream = tridentTopology.newStream(
				INPUT_SPOUT_TX_ID + Math.random() * 100000, spout);
		stream = enhanceTracesStream(stream);

		// Build the actual analysis topology
		TopologyBuilder topologyBuilder = getTopologyBuilder();
		topologyBuilder.build(tridentTopology, spout, stream,
				partitionPersistFactory, persistentAggregateFactory, conf);

		return tridentTopology.build();
	}

	/**
	 * Enhance the incoming kafka stream.
	 * 
	 * <pre>
	 *   Kafka Input:
	 *     [("str"=>"XXXXXXX"),
	 *      ("str"=>"YYYYYYY"), ...]
	 * 
	 *   Enhanced Kafka Stream:
	 *     [("sessionId" => sessionId, "trace" => toMap(JSON.parse(getValue("str") // "XXXXXXX")),
	 *      ("sessionId" => sessionId, "trace" => toMap(JSON.parse(getValue("str") // "YYYYYYY")), ...]
	 * </pre>
	 * 
	 * @param stream
	 *            Default kafka input stream
	 * 
	 * @return An enhanced {@link Stream}
	 */
	public static Stream enhanceTracesStream(Stream stream) {
		return stream.each(new Fields(StringScheme.STRING_SCHEME_KEY),
				new JsonToTrace(), new Fields(TopologyBuilder.TRACE_KEY));
	}

	protected abstract TopologyBuilder getTopologyBuilder();
}
