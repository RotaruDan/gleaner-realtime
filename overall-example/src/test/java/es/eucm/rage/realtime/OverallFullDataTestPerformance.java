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
import es.eucm.rage.realtime.simple.GAnalysis;
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
public class OverallFullDataTestPerformance {

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
		/*
		 * TridentTopology tridentTopology, OpaqueTridentKafkaSpout spout,
		 * Stream tracesStream, StateFactory partitionPersistFactory,
		 * StateFactory persistentAggregateFactory, Map<String, Object> conf
		 */
		// Test topology Builder configuration

		conf.put(AbstractAnalysis.TOPIC_NAME_FLUX_PARAM, TOPIC);

		StormTopology topology = new Analysis().getTopology(conf);
		StormTopology glpTopology = new GAnalysis().getTopology(conf);

		LocalCluster cluster2 = new LocalCluster();
		cluster2.submitTopology("realtime-" + NOW_DATE + "-glp", conf,
				glpTopology);

		LocalCluster cluster3 = new LocalCluster();
		cluster3.submitTopology("realtime-" + NOW_DATE, conf, topology);

		Map<String, Map> results = new HashMap<String, Map>();
		// 1487060745904,completed,level,Atragantamiento,
		// success,True,score,0.7,name,KXIH;
		Map traceMap = new HashMap();
		Map ext = new HashMap();
		Map extt = new HashMap();
		extt.put("time", 1);
		ext.put("event", "completed");
		ext.put("target", "completedchild1");
		ext.put("name", "player1");
		ext.put("score", .7f);
		ext.put("activityId", firstIndex);
		ext.put("success", true);
		ext.put(TopologyBuilder.UUIDV4, UUID.randomUUID().toString());
		ext.put("ext", extt);
		ext.put("timestamp", NOW_DATE);
		ext.put("gameplayId", "gameplay2");
		traceMap.put("out", ext);
		traceMap.put("activityId", firstIndex);
		traceMap.put("gameplayId", "gameplay");
		traceMap.put(TopologyBuilder.UUIDV4, UUID.randomUUID().toString());
		traceMap.put("glpId", analyticsGLPId);
		runProducer(traceMap);
		try {
			Thread.sleep(20000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		Map traceMap2 = new HashMap();
		Map ext2 = new HashMap();
		Map extt2 = new HashMap();
		extt2.put("time", 1);
		ext2.put("event", "completed");
		ext2.put("target", "completedchild2");
		ext2.put("name", "player1");
		ext2.put("activityId", secondIndex);
		ext2.put("score", .7f);
		ext2.put("success", true);
		ext2.put(TopologyBuilder.UUIDV4, UUID.randomUUID().toString());
		ext2.put("ext", extt2);
		ext2.put("gameplayId", "gameplay2");
		traceMap2.put("out", ext2);
		traceMap2.put("activityId", secondIndex);
		traceMap2.put("gameplayId", "gameplay2");
		traceMap2.put("glpId", analyticsGLPId);
		traceMap2.put(TopologyBuilder.UUIDV4, UUID.randomUUID().toString());
		runProducer(traceMap2);

		try {
			Thread.sleep(15000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		String resultsIndex = OverallTopologyBuilder.OVERALL_INDEX
				+ "-progress";

		Response resultResponse = client.performRequest("GET", "/"
				+ resultsIndex + "/" + ESUtils.getResultsType() + "/player1");
		int resultStatus = resultResponse.getStatusLine().getStatusCode();

		assertEquals("TEST GET result error, status is" + resultStatus,
				resultStatus, HttpStatus.SC_OK);

		String responseResultString = EntityUtils.toString(resultResponse
				.getEntity());
		Map<String, Object> playerState = (Map) gson.fromJson(
				responseResultString, Map.class).get("_source");

		Object mProgress = playerState.get("myProgress");

		assertEquals("mProgress doesn't coincide", mProgress, 1.0d);

	}
}