package es.eucm.gleaner.realtime.topologies;

import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class WriteToTextFile implements Filter {

	private String textField;

	private String[] fields;

	public WriteToTextFile(String textField, String... fields) {
		this.textField = textField;
		this.fields = fields;
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(
					textField, true));
			for (String field : fields) {
				writer.write(tuple.getValueByField(field) + ",");
			}
			writer.newLine();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {

	}

	@Override
	public void cleanup() {

	}
}
