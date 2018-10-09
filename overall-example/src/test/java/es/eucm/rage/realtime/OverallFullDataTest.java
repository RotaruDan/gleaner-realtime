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
import es.eucm.rage.realtime.simple.GAnalysis;
import es.eucm.rage.realtime.simple.OAnalysis;
import es.eucm.rage.realtime.simple.topologies.GLPTopologyBuilder;
import es.eucm.rage.realtime.simple.topologies.OverallTopologyBuilder;
import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.utils.CSVToMapTrace;
import es.eucm.rage.realtime.utils.ESUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link OverallTopologyBuilder} executed on a local cluster and
 * receiving data from a local files VERBS_FILES
 */
public class OverallFullDataTest {

	private static final String[] VERBS_FILES = { "DSIR", "KXIH", "SSYP",
			"TQBG", "ZEHU" };

	private static final String NOW_DATE = String.valueOf(new Date().getTime());
	private static final String ES_HOST = "localhost";
	private static final String ZOOKEEPER_URL = "localhost";
	private static final String BOOTSTRAP_SERVERS = "0.0.0.0:9092";
	private static final String TOPIC = "overall-test-topic-default-kibana-analysis-"
			+ NOW_DATE;
	private static final Gson gson = new Gson();

	private static final Producer<Long, String> producer = createProducer();

	private static Producer<Long, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		return new KafkaProducer<>(props);
	}

	static void runProducer(final Map trace) {

		try {
			String msg = gson.toJson(trace, Map.class);
			long index = (long) 0.4;
			final ProducerRecord<Long, String> record = new ProducerRecord<>(
					TOPIC, index, msg);

			producer.send(record).get();

		} catch (Exception e) {
			assertTrue("Error sending to Kafka " + e.getMessage() + " cause "
					+ e.getCause(), false);
		} finally {
			producer.flush();
			// producer.close();
		}
	}

	@Test
	public void xAPIVerbsTest() throws IOException {

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

		Config conf = new Config();
		conf.put(AbstractAnalysis.ZOOKEEPER_URL_FLUX_PARAM, ZOOKEEPER_URL);
		conf.put(AbstractAnalysis.ELASTICSEARCH_URL_FLUX_PARAM, ES_HOST);
		conf.put(AbstractAnalysis.TOPIC_NAME_FLUX_PARAM, TOPIC);
		conf.put(AbstractAnalysis.KAFKA_URL_FLUX_PARAM, ZOOKEEPER_URL + ":9092");
		// Test topology Builder configuration

		StormTopology topology = new OAnalysis().getTopology(conf);
		StormTopology glpTopology = new GAnalysis().getTopology(conf);

		LocalCluster cluster2 = new LocalCluster();
		cluster2.submitTopology("realtime-" + NOW_DATE + "-glp", conf,
				glpTopology);

		LocalCluster cluster3 = new LocalCluster();
		cluster3.submitTopology("realtime-" + NOW_DATE, conf, topology);

		Map<String, Map> results = new HashMap<String, Map>();

		for (int i = 0; i < VERBS_FILES.length; ++i) {
			String idx;
			if (i < 3) {
				idx = firstIndex;
			} else {
				idx = secondIndex;
			}
			List<List<Object>> tuples = parser.getTuples("traces/"
					+ VERBS_FILES[i] + ".csv", idx, i);
			for (List tupleNestedList : tuples) {
				for (Object trace : tupleNestedList) {
					Map traceMap = (Map) trace;

					Map out = (Map) ((Map) trace).get(TopologyBuilder.OUT_KEY);

					Object score = out
							.get(TopologyBuilder.TridentTraceKeys.SCORE);
					if (score != null) {
						float scoreNum = ((Number) score).floatValue();
						String name = out.get(
								TopologyBuilder.TridentTraceKeys.NAME)
								.toString();
						Map player = results.get(name);
						if (player == null) {
							player = new HashMap();
							results.put(name, player);
						}

						Object minObj = player.get("min");
						if (minObj != null) {
							float min = (float) minObj;
							if (scoreNum < min) {
								player.put("min", scoreNum);
							}
						} else {
							player.put("min", scoreNum);
						}

					}
					runProducer(traceMap);
				}
			}
		}

		try {
			Thread.sleep(40000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		List<Map<String, Object>> players = new ArrayList<Map<String, Object>>(
				5);
		Map<String, Object> player0 = new HashMap();
		player0.put("name", "DSIR");
		player0.put("selected", 65);

		players.add(player0);
		Map<String, Object> player1 = new HashMap();
		player1.put("name", "KXIH");
		player1.put("selected", 90);

		players.add(player1);
		Map<String, Object> player2 = new HashMap();
		player2.put("name", "SSYP");
		player2.put("selected", 50);

		players.add(player2);
		Map<String, Object> player3 = new HashMap();
		player3.put("name", "TQBG");
		player3.put("selected", 107);

		players.add(player3);
		Map<String, Object> player4 = new HashMap();
		player4.put("name", "ZEHU");
		player4.put("selected", 44);

		players.add(player4);

		String resultsIndex = OverallTopologyBuilder.OVERALL_INDEX;
		for (int i = 0; i < players.size(); ++i) {

			Map player = players.get(i);
			String name = player.get("name").toString();
			// Check responses

			Response resultResponse = client.performRequest("GET", "/"
					+ resultsIndex + "/" + ESUtils.getResultsType() + "/"
					+ name);
			int resultStatus = resultResponse.getStatusLine().getStatusCode();

			assertEquals("TEST GET result error, status is" + resultStatus,
					resultStatus, HttpStatus.SC_OK);

			String responseResultString = EntityUtils.toString(resultResponse
					.getEntity());
			Map<String, Object> playerState = (Map) gson.fromJson(
					responseResultString, Map.class).get("_source");

			List<Map> answers = (List) playerState.get("answers");

			assertEquals("Answers don't coincide", answers.size(),
					player.get("selected"));
		}
	}
}