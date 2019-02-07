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
import es.eucm.rage.realtime.simple.PAnalysis;
import es.eucm.rage.realtime.simple.topologies.PerformanceTopologyBuilder;
import es.eucm.rage.realtime.topologies.TopologyBuilder;
import es.eucm.rage.realtime.utils.CSVToMapTrace;
import es.eucm.rage.realtime.utils.ESUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.org.apache.commons.collections.map.HashedMap;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link TopologyBuilder} executed on a local cluster and receiving
 * data from a local files TRACES_FILES
 */
public class PerformanceDataTest {

	private static final String[] TRACES_FILES = { "DSIR", "KXIH", "SSYP",
			"TQBG", "ZEHU" };
	private static final String NOW_DATE = String.valueOf(
			(new SimpleDateFormat("dd_MM_yyyy_HH_mm_ss").format(new Date())))
			.toLowerCase();
	private static final String ES_HOST = "localhost";
	private static final String ZOOKEEPER_URL = "localhost";
	private static final String BOOTSTRAP_SERVERS = "0.0.0.0:9092";
	private static final String TOPIC = "performance-test-topic-default-kibana-analysis-"
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
			// producer.close();
		}
	}

	@Test
	public void test() throws IOException {

		CSVToMapTrace parser = new CSVToMapTrace("testglpid" + NOW_DATE);
		String firstIndex = "1-" + NOW_DATE;
		String secondIndex = "2-" + NOW_DATE;

		for (int i = 0; i < TRACES_FILES.length; ++i) {
			String idx;
			if (i < 3) {
				idx = firstIndex;
			} else {
				idx = secondIndex;
			}
			List<List<Object>> tuples = parser.getTuples("performance/"
					+ TRACES_FILES[i] + ".csv", idx, i);
			for (List tupleNestedList : tuples) {
				for (Object trace : tupleNestedList) {
					Map traceMap = (Map) trace;
					runProducer(traceMap);
				}
			}
		}
		/*
		 * new Thread(new Runnable() {
		 * 
		 * @Override public void run() {
		 * 
		 * try { runConsumer(firstChildTraces, secondChildTraces,
		 * parentChildTraces, firstIndex, secondIndex, parentIndex); } catch
		 * (InterruptedException e) { e.printStackTrace(); } } }).start();
		 */
		Map<String, Object> conf = new HashedMap();
		conf.put(AbstractAnalysis.ELASTICSEARCH_URL_FLUX_PARAM, ES_HOST);
		conf.put(AbstractAnalysis.ZOOKEEPER_URL_FLUX_PARAM, ZOOKEEPER_URL);
		conf.put(AbstractAnalysis.TOPIC_NAME_FLUX_PARAM, TOPIC);

		StormTopology topology = new PAnalysis().getTopology(conf);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("realtime-" + NOW_DATE, conf, topology);
		try {
			Thread.sleep(35000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		String resultsIndex = PerformanceTopologyBuilder.PERFORMANCE_INDEX;

		RestClient client = RestClient.builder(new HttpHost(ES_HOST, 9200))
				.build();
		Response resultResponse = client.performRequest("GET", "/"
				+ resultsIndex + "/" + ESUtils.getResultsType() + "/testClass");
		int resultStatus = resultResponse.getStatusLine().getStatusCode();

		assertEquals("TEST GET result error, status is" + resultStatus,
				resultStatus, HttpStatus.SC_OK);

		String responseResultString = EntityUtils.toString(resultResponse
				.getEntity());
		Map<String, Object> playerState = (Map) gson.fromJson(
				responseResultString, Map.class).get("_source");

		Map yearMap = (Map) playerState.get("2017");

		Map months = (Map) yearMap.get("months");
		Map first = (Map) months.get("1");
		List firstMonthStudents = (List) first.get("students");

		assertEquals("year doesn't coincide", firstMonthStudents.size(), 5);
		Map weeks = (Map) yearMap.get("weeks");
		Map week7 = (Map) weeks.get("7");
		List week7Students = (List) week7.get("students");
		assertEquals("year doesn't coincide", week7Students.size(), 5);

		List students = (List) yearMap.get("students");
		assertEquals("year doesn't coincide", students.size(), 5);

	}
}
