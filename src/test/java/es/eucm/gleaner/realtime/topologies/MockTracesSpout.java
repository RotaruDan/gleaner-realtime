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
package es.eucm.gleaner.realtime.topologies;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import com.mongodb.BasicDBObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class MockTracesSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;

	private ArrayList<String> lines;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("versionId", "trace"));
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		this.lines = new ArrayList<String>();

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				ClassLoader.getSystemResourceAsStream("testtraces.txt")));

		String line;
		try {
			while ((line = reader.readLine()) != null) {
				lines.add(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void nextTuple() {
		if (!lines.isEmpty()) {
			String line = lines.remove(0);
			String[] values = line.split(",");
			int i = 0;
			String version = values[i++];
			BasicDBObject trace = new BasicDBObject();
			trace.put("gameplayId", values[i++]);
			trace.put("event", values[i++]);
			trace.put("target", values[i++]);
			trace.put("value", values[i++]);
			collector.emit(Arrays.<Object> asList(version, trace));
		}
	}

	public boolean isDone() {
		return lines != null && lines.isEmpty();
	}
}
