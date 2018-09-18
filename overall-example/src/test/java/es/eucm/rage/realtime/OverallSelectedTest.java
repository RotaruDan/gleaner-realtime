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
import es.eucm.rage.realtime.simple.OAnalysis;
import es.eucm.rage.realtime.simple.topologies.OverallTopologyBuilder;
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
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.FeederBatchSpout;
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
public class OverallSelectedTest {

	private static final String[] VERBS_FILES = { "alternative/selected" };

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

		String firstIndex = "1-" + NOW_DATE;
		String secondIndex = "2-" + NOW_DATE;
		String middleIndex = "middle-" + NOW_DATE;
		String parentIndex = "parent-" + NOW_DATE; // GLP_ID
		String analyticsGLPId = ESUtils.getAnalyticsGLPIndex(parentIndex);

		RestClient client = RestClient.builder(new HttpHost(ES_HOST, 9200))
				.build();
		RestHighLevelClient hClient = new RestHighLevelClient(client);

		Map parentAnalytics = new HashMap();
		parentAnalytics.put("name", "parent");
		IndexRequest indexParent = new IndexRequest(analyticsGLPId,
				"analytics", parentIndex).source(parentAnalytics);

		// MIDDLE CHILD
		Map middleAnalytics = new HashMap();
		middleAnalytics.put("name", "middle");
		IndexRequest indexMiddle = new IndexRequest(analyticsGLPId,
				"analytics", middleIndex).source(middleAnalytics);

		Map firstChildAnalytics = new HashMap();
		firstChildAnalytics.put("name", "first child");
		IndexRequest indexFirstChild = new IndexRequest(analyticsGLPId,
				"analytics", firstIndex).source(firstChildAnalytics);

		Map secondChildAnalytics = new HashMap();
		secondChildAnalytics.put("name", "second child");
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

		StormTopology topology = new OAnalysis().getTopology(conf);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("realtime", conf, topology);

		for (int i = 0; i < VERBS_FILES.length; ++i) {
			String idx;
			if (i < 3) {
				idx = firstIndex;
			} else {
				idx = secondIndex;
			}
			List<List<Object>> tuples = parser.getTuples("verbs/"
					+ VERBS_FILES[i] + ".csv", idx, i);
			for (List tupleNestedList : tuples) {
				for (Object trace : tupleNestedList) {
					Map traceMap = (Map) trace;
					runProducer(traceMap);
				}
			}
		}

		try {
			Thread.sleep(25000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		String resultsIndex = OverallTopologyBuilder.OVERALL_INDEX;
		for (int i = 0; i < VERBS_FILES.length; ++i) {

			// Check responses

			List<String> lines = parser.getLines("verbs/" + VERBS_FILES[i]
					+ ".csv");

			Response resultResponse = client.performRequest("GET", "/"
					+ resultsIndex + "/" + ESUtils.getResultsType() + "/CRBT;");
			int resultStatus = resultResponse.getStatusLine().getStatusCode();

			assertEquals("TEST GET result error, status is" + resultStatus,
					resultStatus, HttpStatus.SC_OK);

			String responseResultString = EntityUtils.toString(resultResponse
					.getEntity());
			Map<String, Object> playerState = (Map) gson.fromJson(
					responseResultString, Map.class).get("_source");

			List<Map> answers = (List) playerState.get("answers");

			assertEquals("Answers don't coincide", answers.size(), lines.size());

			assertEquals("Incorrect should be 5", playerState.get("incorrect"),
					5d);
			assertEquals("correct should be 7", playerState.get("correct"), 8d);

		}
	}
}