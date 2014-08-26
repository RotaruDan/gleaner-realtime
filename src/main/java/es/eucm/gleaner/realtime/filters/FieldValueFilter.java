package es.eucm.gleaner.realtime.filters;

import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

public class FieldValueFilter implements Filter {

    private String field;

	private Object value;

	public FieldValueFilter(String field, Object value) {
        this.field = field;
        this.value = value;
	}

	@Override
	public boolean isKeep(TridentTuple objects) {
        return value.equals(objects.getValueByField("event"));
	}

	@Override
	public void prepare(Map map, TridentOperationContext tridentOperationContext) {

	}

	@Override
	public void cleanup() {

	}
}
