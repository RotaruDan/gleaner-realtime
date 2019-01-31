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
import es.eucm.rage.realtime.simple.DAnalysis;
import es.eucm.rage.realtime.simple.GAnalysis;
import es.eucm.rage.realtime.simple.topologies.GLPTopologyBuilder;
import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.utils.CSVToMapTrace;
import es.eucm.rage.realtime.utils.ESUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link TopologyBuilder} executed on a local cluster and receiving
 * data from a local files TRACES_FILES
 */
public class DefaultAnalysisAggregation {

	private static final String[] TRACES_FILES = { "defaultAggregation" };
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

	/**
	 * Returns a Producer (conexion with Kafka) that sends data to the queue
	 * 
	 * @return the producer created
	 */
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

	/**
	 * Receives a Trace Map and feeds the map to Kafka with the help of the
	 * producer
	 * 
	 * @param trace
	 */
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
		String parentIndex = "parent-" + NOW_DATE; // GLP_ID
		String analyticsGLPId = ESUtils.getAnalyticsGLPIndex(parentIndex);

		RestClient client = RestClient.builder(new HttpHost(ES_HOST, 9200))
				.build();
		RestHighLevelClient hClient = new RestHighLevelClient(client);

		Map parentAnalytics = new HashMap();
		parentAnalytics.put("testkey", "testval");
		ArrayList childrenArray = new ArrayList<>(1);
		childrenArray.add(firstIndex);
		childrenArray.add(secondIndex);
		parentAnalytics.put(GLPTopologyBuilder.PARENT_CHILDREN_TRACE,
				childrenArray);

		IndexRequest indexParent = new IndexRequest(analyticsGLPId,
				"analytics", parentIndex).source(parentAnalytics);

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
				parentIndex);
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
				parentIndex);
		IndexRequest indexSecondChild = new IndexRequest(analyticsGLPId,
				"analytics", secondIndex).source(secondChildAnalytics);

		hClient.index(indexParent);
		hClient.index(indexFirstChild);
		hClient.index(indexSecondChild);

		// First weight "+"
		Map parentAnalyticsWeights = new HashMap();
		ArrayList weightsArray = new ArrayList<>(1);

		Map childWeight1 = new HashMap();
		childWeight1.put(GLPTopologyBuilder.OPERATION_CHILD_ID_KEY, firstIndex);
		childWeight1.put(GLPTopologyBuilder.OPERATION_NAME_KEY, "score");
		childWeight1.put(GLPTopologyBuilder.OPERATION_CHILD_MULTIPLIER_KEY,
				0.5f);

		Map childWeight2 = new HashMap();
		childWeight2
				.put(GLPTopologyBuilder.OPERATION_CHILD_ID_KEY, secondIndex);
		childWeight2.put(GLPTopologyBuilder.OPERATION_NAME_KEY, "score");
		childWeight2.put(GLPTopologyBuilder.OPERATION_CHILD_MULTIPLIER_KEY,
				0.9f);

		Map firstWeight = new HashMap();
		firstWeight.put(GLPTopologyBuilder.OPERATION_CHILDREN_KEY,
				Arrays.asList(childWeight1, childWeight2));
		firstWeight.put(GLPTopologyBuilder.OPERATION_KEY, "+");
		String resultVarName = "resultAggregationValue";
		firstWeight.put(GLPTopologyBuilder.OPERATION_NAME_KEY, resultVarName);

		// Second weight "*"

		Map childWeight12 = new HashMap();
		childWeight12
				.put(GLPTopologyBuilder.OPERATION_CHILD_ID_KEY, firstIndex);
		childWeight12.put(GLPTopologyBuilder.OPERATION_NAME_KEY, "score");
		childWeight12.put(GLPTopologyBuilder.OPERATION_CHILD_MULTIPLIER_KEY,
				0.9f);

		Map childWeight22 = new HashMap();
		childWeight22.put(GLPTopologyBuilder.OPERATION_CHILD_ID_KEY,
				secondIndex);
		childWeight22.put(GLPTopologyBuilder.OPERATION_NAME_KEY, "score");
		childWeight22.put(GLPTopologyBuilder.OPERATION_CHILD_MULTIPLIER_KEY,
				0.9f);

		Map secondWeight = new HashMap();
		secondWeight.put(GLPTopologyBuilder.OPERATION_CHILDREN_KEY,
				Arrays.asList(childWeight12, childWeight22));
		secondWeight.put(GLPTopologyBuilder.OPERATION_KEY, "*");
		String resultVarName2 = "resultMultAggregationValue";
		secondWeight.put(GLPTopologyBuilder.OPERATION_NAME_KEY, resultVarName2);

		weightsArray.add(firstWeight);
		weightsArray.add(secondWeight);

		parentAnalyticsWeights.put(GLPTopologyBuilder.WEIGHTS, weightsArray);

		IndexRequest indexParentWeights = new IndexRequest(analyticsGLPId,
				"analytics", "weights_" + parentIndex)
				.source(parentAnalyticsWeights);
		hClient.index(indexParentWeights);

		// Parser that receives a local file in CSV and returns a list of Map
		// Traces
		CSVToMapTrace parser = new CSVToMapTrace(analyticsGLPId);
		// Read the TEST files, parse them and Feed them to the Local Cluster
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
				}
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
			// Wait for thr traces to be analyzed by the Topology
			Thread.sleep(20000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// Start querying ElasticSearch and compare the results with the
		// expected values
		Response response = client.performRequest(
				"GET",
				"/"
						+ ESUtils.getResultsIndex(ESUtils
								.getRootGLPId(analyticsGLPId)) + "/results/"
						+ "agg_" + parentIndex + "_USMK");
		int status = response.getStatusLine().getStatusCode();

		assertEquals("TEST GET error, status is" + status, status,
				HttpStatus.SC_OK);

		String responseString = EntityUtils.toString(response.getEntity());
		Map<String, Object> responseDocs = (Map) gson.fromJson(responseString,
				Map.class);

		responseDocs = (Map) responseDocs.get("_source");
		float finalValueResult = Float.parseFloat(responseDocs.get(
				resultVarName).toString());
		float finalValueResult2 = Float.parseFloat(responseDocs.get(
				resultVarName2).toString());

		assertEquals("finalValueResult " + resultVarName + "is "
				+ finalValueResult, 0.35f, finalValueResult, 0.001f);

		assertEquals("finalValueResult " + resultVarName2 + "is "
				+ finalValueResult2, 0f, finalValueResult2, 0.001f);

		response = client.performRequest(
				"GET",
				"/"
						+ ESUtils.getResultsIndex(ESUtils
								.getRootGLPId(analyticsGLPId)) + "/results/"
						+ "agg_" + firstIndex + "_USMK");
		status = response.getStatusLine().getStatusCode();

		assertEquals("TEST GET error, status is" + status, status,
				HttpStatus.SC_OK);

		responseString = EntityUtils.toString(response.getEntity());
		responseDocs = (Map) gson.fromJson(responseString, Map.class);

		responseDocs = (Map) responseDocs.get("_source");
		finalValueResult = Float.parseFloat(responseDocs.get("score")
				.toString());
		assertEquals("finalValueResult (score) is " + finalValueResult, 0.7f,
				finalValueResult, 0.001f);

		response = client.performRequest("GET", "/" + parentIndex
				+ "/_search?size=5000&q=*:*");
		status = response.getStatusLine().getStatusCode();

		assertEquals("TEST GET error, status is" + status, status,
				HttpStatus.SC_OK);

		responseString = EntityUtils.toString(response.getEntity());
		responseDocs = (Map) gson.fromJson(responseString, Map.class);

		Map hits = (Map) responseDocs.get("hits");

		int total = ((Double) hits.get("total")).intValue();

		assertEquals("Total traces " + parentIndex + ", current " + total, 3,
				total);

		List<List<Object>> tuples = parser.getTuples("glp/" + TRACES_FILES[0]
				+ ".csv", secondIndex, 2);

		// Send second batch of traces to the Topology (kafka)
		for (List tupleNestedList : tuples) {
			for (Object trace : tupleNestedList) {
				Map traceMap = (Map) trace;
				runProducer(traceMap);
			}
		}

		try {
			// Wait for thr traces to be analyzed by the Topology
			Thread.sleep(20000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// Start querying ElasticSearch and compare the results with the
		// expected values
		response = client.performRequest(
				"GET",
				"/"
						+ ESUtils.getResultsIndex(ESUtils
								.getRootGLPId(analyticsGLPId)) + "/results/"
						+ "agg_" + parentIndex + "_USMK");
		status = response.getStatusLine().getStatusCode();

		assertEquals("TEST GET error, status is" + status, status,
				HttpStatus.SC_OK);

		responseString = EntityUtils.toString(response.getEntity());
		responseDocs = (Map) gson.fromJson(responseString, Map.class);

		responseDocs = (Map) responseDocs.get("_source");
		finalValueResult = Float.parseFloat(responseDocs.get(resultVarName)
				.toString());
		finalValueResult2 = Float.parseFloat(responseDocs.get(resultVarName2)
				.toString());

		assertEquals("finalValueResult " + resultVarName + "is "
				+ finalValueResult, 0.7f * 0.5f + 0.7f * 0.9f,
				finalValueResult, 0.001f);

		assertEquals("finalValueResult " + resultVarName2 + "is "
				+ finalValueResult2, 0.7f * 0.9f * 0.7f * 0.9f,
				finalValueResult2, 0.001f);

		response = client.performRequest(
				"GET",
				"/"
						+ ESUtils.getResultsIndex(ESUtils
								.getRootGLPId(analyticsGLPId)) + "/results/"
						+ "agg_" + firstIndex + "_USMK");
		status = response.getStatusLine().getStatusCode();

		assertEquals("TEST GET error, status is" + status, status,
				HttpStatus.SC_OK);

		responseString = EntityUtils.toString(response.getEntity());
		responseDocs = (Map) gson.fromJson(responseString, Map.class);

		responseDocs = (Map) responseDocs.get("_source");
		finalValueResult = Float.parseFloat(responseDocs.get("score")
				.toString());
		assertEquals("finalValueResult (score) is " + finalValueResult, 0.7f,
				finalValueResult, 0.001f);

		response = client.performRequest(
				"GET",
				"/"
						+ ESUtils.getResultsIndex(ESUtils
								.getRootGLPId(analyticsGLPId)) + "/results/"
						+ "agg_" + secondIndex + "_USMK");
		status = response.getStatusLine().getStatusCode();

		assertEquals("TEST GET error, status is" + status, status,
				HttpStatus.SC_OK);

		responseString = EntityUtils.toString(response.getEntity());
		responseDocs = (Map) gson.fromJson(responseString, Map.class);

		responseDocs = (Map) responseDocs.get("_source");
		assertEquals("finalValueResult (score) is " + finalValueResult, 0.7f,
				Float.parseFloat(responseDocs.get("score").toString()), 0.001f);
	}
}
