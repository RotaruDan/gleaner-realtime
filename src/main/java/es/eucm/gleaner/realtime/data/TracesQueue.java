package es.eucm.gleaner.realtime.data;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;

import java.util.List;

public class TracesQueue {

	public static int MAX_TRACES = 100;

    private String versionId;

	private DBCollection collection;

    private AnalysisState analysisState;

	private List<DBObject> tracesQueue;

    public TracesQueue(DB db, String versionId, AnalysisState analysisState) {
		collection = db.getCollection("traces_" + versionId);
		this.versionId = versionId;
		this.analysisState = analysisState;
        refresh();
	}

	private void refresh() {
		String lastId = analysisState.getRealtimeId(versionId);
		ObjectId id = null;
		if (lastId != null) {
			id = new ObjectId(lastId);
		}

		DBObject query = id == null ? new BasicDBObject() : new BasicDBObject(
				"_id", new BasicDBObject("$gt", id));

		tracesQueue = collection.find(query).limit(MAX_TRACES).toArray();
	}

	public DBObject poll() {
		if (tracesQueue.isEmpty()) {
			refresh();
		}

		if (tracesQueue.isEmpty()) {
			return null;
		} else {
			DBObject dbObject = tracesQueue.remove(0);
			return dbObject;
		}
	}

    public boolean isEmpty() {
        if (tracesQueue.isEmpty()){
            refresh();
        }
        return tracesQueue.isEmpty();
    }

    public String getVersionId() {
        return versionId;
    }
}
