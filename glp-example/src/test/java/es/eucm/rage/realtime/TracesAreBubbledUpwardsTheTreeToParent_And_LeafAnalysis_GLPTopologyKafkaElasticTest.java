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

import com.google.gson.Gson;
import es.eucm.rage.realtime.simple.Analysis;
import es.eucm.rage.realtime.simple.DAnalysis;
import es.eucm.rage.realtime.simple.GAnalysis;
import es.eucm.rage.realtime.simple.topologies.GLPTopologyBuilder;
import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.utils.CSVToMapTrace;
import es.eucm.rage.realtime.utils.ESUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.org.apache.commons.collections.map.HashedMap;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link TopologyBuilder} executed on a local cluster and receiving
 * data from a local files TRACES_FILES
 */
public class TracesAreBubbledUpwardsTheTreeToParent_And_LeafAnalysis_GLPTopologyKafkaElasticTest {

	private static final String[] TRACES_FILES = { "1" };
	private static final String NOW_DATE = String.valueOf(
			(new SimpleDateFormat("dd_MM_yyyy_HH_mm_ss").format(new Date())))
			.toLowerCase();
	private static final String ES_HOST = "localhost";
	private static final String ZOOKEEPER_URL = "localhost";
	private static final String BOOTSTRAP_SERVERS = "0.0.0.0:9092";
	private static final String TOPIC = "glp-test-topic-default-kibana-analysis-"
			+ NOW_DATE;
	private static final Producer<String, String> producer = createProducer();

	private static final Gson gson = new Gson();

	private static Producer<String, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		return new KafkaProducer<>(props);
	}

	static void runProducer(final Map trace) {

		try {
			String msg = gson.toJson(trace, Map.class);
			String index = "test";
			final ProducerRecord<String, String> record = new ProducerRecord<>(
					TOPIC, index, msg);

			producer.send(record).get();

		} catch (Exception e) {
			assertTrue("Error sending to Kafka " + e.getMessage() + " cause "
					+ e.getCause(), false);
		} finally {
			producer.flush();
		}
	}

	@Test
	public void test() throws Exception {

		/**
		 * Simple GLP: PARENT: "parent-" + NOW_DATE -> GLP_ID
		 * 
		 * FIRST CHILD: "1-" + NOW_DATE -> leaf -> receives traces SECOND CHILD:
		 * "2-" + NOW_DATE -> leaf -> receives traces
		 * **/
		String firstIndex = "1-" + NOW_DATE;
		String secondIndex = "2-" + NOW_DATE;
		String middleIndex = "middle-" + NOW_DATE;
		String parentIndex = "parent-" + NOW_DATE; // GLP_ID
		String analyticsGLPId = ESUtils.getAnalyticsGLPIndex(parentIndex);

		RestClient client = RestClient.builder(new HttpHost(ES_HOST, 9200))
				.build();
		RestHighLevelClient hClient = new RestHighLevelClient(client);

		Map parentAnalytics = new HashMap();
		parentAnalytics.put("testkey", "testval");
		ArrayList childrenArray = new ArrayList<>(1);
		childrenArray.add(middleIndex);
		parentAnalytics.put(GLPTopologyBuilder.PARENT_CHILDREN_TRACE,
				childrenArray);
		IndexRequest indexParent = new IndexRequest(analyticsGLPId,
				"analytics", parentIndex).source(parentAnalytics);

		// MIDDLE CHILD
		Map middleAnalytics = new HashMap();
		ArrayList middleArray = new ArrayList<>(2);
		middleArray.add(firstIndex);
		middleArray.add(secondIndex);
		middleAnalytics.put(GLPTopologyBuilder.PARENT_CHILDREN_TRACE,
				middleArray);
		middleAnalytics.put(TopologyBuilder.ANALYTICS_PARENT_ID_KEY,
				parentIndex);
		IndexRequest indexMiddle = new IndexRequest(analyticsGLPId,
				"analytics", middleIndex).source(middleAnalytics);

		Map firstChildAnalytics = new HashMap();

		// Limits
		Map firstChildLimits = new HashMap();
		Map firstChildPartialThresholds = new HashMap();
		firstChildPartialThresholds.put(
				GLPTopologyBuilder.PARTIAL_THRESHOLD_SCORE, 0.3f);
		firstChildPartialThresholds.put(GLPTopologyBuilder.LEARNING_OBJECTIVES,
				0.3f);
		firstChildPartialThresholds.put(GLPTopologyBuilder.COMPETENCIES, 0.3f);
		firstChildLimits.put(GLPTopologyBuilder.PARTIAL_THRESHOLDS,
				firstChildPartialThresholds);
		firstChildAnalytics.put(GLPTopologyBuilder.LIMITS, firstChildLimits);

		// Contributes
		Map firstChildContributes = new HashMap();
		Map firstChildContributesCompetencies = new HashMap();
		firstChildContributesCompetencies.put("child1_comp1", 0.1f);
		firstChildContributesCompetencies.put("child1_comp2", 0.1f);
		firstChildContributes.put(GLPTopologyBuilder.COMPETENCIES,
				firstChildContributesCompetencies);
		Map firstChildContributesLearningObjectives = new HashMap();
		firstChildContributesLearningObjectives.put("child1_lo1", 0.2f);
		firstChildContributes.put(GLPTopologyBuilder.LEARNING_OBJECTIVES,
				firstChildContributesLearningObjectives);
		firstChildAnalytics.put(GLPTopologyBuilder.CONTRIBUTES,
				firstChildContributes);

		firstChildAnalytics.put(TopologyBuilder.ANALYTICS_PARENT_ID_KEY,
				middleIndex);
		IndexRequest indexFirstChild = new IndexRequest(analyticsGLPId,
				"analytics", firstIndex).source(firstChildAnalytics);

		Map secondChildAnalytics = new HashMap();

		// Limits
		Map secondChildLimits = new HashMap();
		Map secondChildPartialThresholds = new HashMap();
		secondChildPartialThresholds.put(
				GLPTopologyBuilder.PARTIAL_THRESHOLD_SCORE, 0.5f);
		secondChildPartialThresholds.put(
				GLPTopologyBuilder.LEARNING_OBJECTIVES, 0.5f);
		secondChildPartialThresholds.put(GLPTopologyBuilder.COMPETENCIES, 0.5f);
		secondChildLimits.put(GLPTopologyBuilder.PARTIAL_THRESHOLDS,
				secondChildPartialThresholds);
		secondChildAnalytics.put(GLPTopologyBuilder.LIMITS, secondChildLimits);

		// Contributes
		Map secondChildContributes = new HashMap();
		Map secondChildContributesCompetencies = new HashMap();
		secondChildContributesCompetencies.put("child2_comp1", 0.6f);
		secondChildContributes.put(GLPTopologyBuilder.COMPETENCIES,
				secondChildContributesCompetencies);
		Map secondChildContributesLearningObjectives = new HashMap();
		secondChildContributesLearningObjectives.put("child2_lo1", 0.7f);
		secondChildContributes.put(GLPTopologyBuilder.LEARNING_OBJECTIVES,
				secondChildContributesLearningObjectives);
		secondChildAnalytics.put(GLPTopologyBuilder.CONTRIBUTES,
				secondChildContributes);

		secondChildAnalytics.put(TopologyBuilder.ANALYTICS_PARENT_ID_KEY,
				middleIndex);
		IndexRequest indexSecondChild = new IndexRequest(analyticsGLPId,
				"analytics", secondIndex).source(secondChildAnalytics);

		hClient.index(indexParent);
		hClient.index(indexMiddle);
		hClient.index(indexFirstChild);
		hClient.index(indexSecondChild);

		CSVToMapTrace parser = new CSVToMapTrace(analyticsGLPId);
		Map<String, Integer> res = new HashMap<>();
		List<Map> firstChildTraces = new ArrayList<>();
		List<Map> secondChildTraces = new ArrayList<>();
		List<Map> parentChildTraces = new ArrayList<>();
		List<List<Object>> tuples2 = null;
		for (int i = 0; i < TRACES_FILES.length; ++i) {
			String idx;
			if (i < 3) {
				idx = firstIndex;
			} else {
				idx = secondIndex;
			}
			List<List<Object>> tuples = parser.getTuples("glp/"
					+ TRACES_FILES[i] + ".csv", idx, i);
			for (List tupleNestedList : tuples) {
				for (Object trace : tupleNestedList) {
					Map traceMap = (Map) trace;
					runProducer(traceMap);

					if (i < 3) {
						firstChildTraces.add(traceMap);
					} else {
						secondChildTraces.add(traceMap);
					}
					parentChildTraces.add(traceMap);
				}
			}
			tuples2 = parser.getTuples("glp/" + TRACES_FILES[i] + ".csv",
					secondIndex, 2);

			Integer current = res.get(idx);
			if (current == null) {
				res.put(idx, parser.getCompletedSuccessfully());
			} else {
				res.put(idx, current + parser.getCompletedSuccessfully());
			}

			Integer currentParent = res.get(parentIndex);
			if (currentParent == null) {
				res.put(parentIndex, parser.getCompletedSuccessfully());
			} else {
				res.put(parentIndex,
						currentParent + parser.getCompletedSuccessfully());
			}
		}

		Map<String, Object> conf = new HashedMap();
		conf.put(AbstractAnalysis.ELASTICSEARCH_URL_FLUX_PARAM, ES_HOST);
		conf.put(AbstractAnalysis.ZOOKEEPER_URL_FLUX_PARAM, ZOOKEEPER_URL);
		conf.put(AbstractAnalysis.KAFKA_URL_FLUX_PARAM, ZOOKEEPER_URL + ":9092");
		conf.put(AbstractAnalysis.TOPIC_NAME_FLUX_PARAM, TOPIC);

		StormTopology topology = new GAnalysis().getTopology(conf);
		StormTopology defaultTopology = new DAnalysis().getTopology(conf);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("realtime-" + NOW_DATE + "-default", conf,
				defaultTopology);

		LocalCluster cluster2 = new LocalCluster();
		cluster2.submitTopology("realtime-" + NOW_DATE, conf, topology);

		try {
			Thread.sleep(20000);

			for (List tupleNestedList : tuples2) {
				for (Object trace : tupleNestedList) {
					Map traceMap = (Map) trace;
					runProducer(traceMap);
				}
			}
			Thread.sleep(25000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Response response = client.performRequest("GET", "/" + parentIndex
				+ "/_search?size=5000&q=*:*");
		int status = response.getStatusLine().getStatusCode();

		assertEquals("TEST GET error, status is" + status, status,
				HttpStatus.SC_OK);

		String responseString = EntityUtils.toString(response.getEntity());
		Map<String, Object> responseDocs = (Map) gson.fromJson(responseString,
				Map.class);

		Map hits = (Map) responseDocs.get("hits");

		int total = ((Double) hits.get("total")).intValue();

		assertEquals("Total traces " + parentIndex + ", current " + total, 112,
				total);
	}
}
