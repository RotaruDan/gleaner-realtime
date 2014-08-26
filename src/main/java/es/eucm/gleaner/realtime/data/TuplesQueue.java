package es.eucm.gleaner.realtime.data;

import com.mongodb.DB;
import com.mongodb.DBObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TuplesQueue {

	public static int MAX_NEW_QUEUES = 10;

	public static int TUPLES_PER_QUEUE = 10;

	private DB db;

	private int currentIndex = 0;

	private int currentCount = 0;

	private PendingRTAnalysis pendingRTAnalysis;

	private AnalysisState analysisState;

	private Map<String, TracesQueue> queues = new HashMap<String, TracesQueue>();

	private ArrayList<TracesQueue> orderedQueues = new ArrayList<TracesQueue>();

	public TuplesQueue(DB db, PendingRTAnalysis pendingRTAnalysis,
			AnalysisState analysisState) {
		this.pendingRTAnalysis = pendingRTAnalysis;
		this.analysisState = analysisState;
		this.db = db;
	}

	/**
	 * @return an array, first member is the id of the tuple, second, the tuple
	 *         itself
	 */
	public Object[] nextTuple() {
		refreshQueues();
		TracesQueue queue = nextQueueWithTraces();
		if (queue != null) {
			DBObject object = queue.poll();
			currentCount++;
			if (currentCount >= TUPLES_PER_QUEUE) {
				incindex();
			}
			return new Object[] { object.get("_id"),
					Arrays.asList(queue.getVersionId(), object.toMap()) };
		}
		return null;
	}

	private void refreshQueues() {
		int i = 0;
		String versionId;
		while ((versionId = pendingRTAnalysis.pop()) != null
				&& i < MAX_NEW_QUEUES) {
			if (!queues.containsKey(versionId)) {
				TracesQueue tracesQueue = new TracesQueue(db, versionId,
						analysisState);
				queues.put(versionId, tracesQueue);
				orderedQueues.add(tracesQueue);
			}
			i++;
		}
	}

	private TracesQueue nextQueueWithTraces() {
		if (orderedQueues.isEmpty()) {
			return null;
		}

		if (!orderedQueues.get(currentIndex).isEmpty()) {
			return orderedQueues.get(currentIndex);
		}

		currentCount = 0;
		int index = currentIndex;
		TracesQueue tracesQueue;
		do {
			incindex();
			tracesQueue = orderedQueues.get(currentIndex);
		} while (currentIndex != index && tracesQueue.isEmpty());
		return tracesQueue.isEmpty() ? null : tracesQueue;
	}

	private void incindex() {
		currentIndex = (currentIndex + 1) % orderedQueues.size();
	}

	public void updateLasId(String versionId, String traceId) {
		analysisState.setRealtimeId(versionId, traceId);
	}
}
