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
