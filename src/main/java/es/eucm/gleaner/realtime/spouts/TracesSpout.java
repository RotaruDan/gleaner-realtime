package es.eucm.gleaner.realtime.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.mongodb.DB;
import es.eucm.gleaner.realtime.data.AnalysisState;
import es.eucm.gleaner.realtime.data.PendingRTAnalysis;
import es.eucm.gleaner.realtime.data.TuplesQueue;
import es.eucm.gleaner.realtime.utils.DBUtils;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;

public class TracesSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;

	private TuplesQueue tuplesQueue;

	public TracesSpout() {
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		DB db = DBUtils.getMongoDB(conf);
		Jedis jedis = DBUtils.getJedis(conf);
		this.tuplesQueue = new TuplesQueue(db, new PendingRTAnalysis(jedis,
				"q_realtime"), new AnalysisState(jedis));

	}

	@Override
	public void close() {
	}

	@Override
	public void nextTuple() {
		Object[] tuple = tuplesQueue.nextTuple();
		if (tuple == null) {
			Utils.sleep(1000);
		} else {
			List<Object> t = (List<Object>) tuple[1];
			String versionId = t.get(0).toString();
			this.collector.emit(t, versionId + ";" + tuple[0]);
		}
	}

	@Override
	public void ack(Object msgId) {
		String[] split = msgId.toString().split(";");
		tuplesQueue.updateLasId(split[0], split[1]);
	}

	@Override
	public void fail(Object msgId) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("versionId", "trace"));
	}
}
