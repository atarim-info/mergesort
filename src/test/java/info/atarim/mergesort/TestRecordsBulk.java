package info.atarim.mergesort;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRecordsBulk {
	private static final Logger LOG = LoggerFactory.getLogger(TestRecordsBulk.class );

	@Test
	public void test() {
		String[] records = new String[] {
			"111,1",
			"333,3",
			"222,2",
			"000,0"
		};
		
		LOG.debug("recordsList");
		RecordsBulk recordsBulk = new RecordsBulk();
		for (int i = 0; i < records.length; i++) {
			Record record = new Record(records[i]);
			LOG.debug(record.asString());
			recordsBulk.addRecord(record);
			
		}
		List<Record> recordsList = recordsBulk.getRecords();
		recordsBulk.sort(1);
		
		List<Record> sortedRecordsList = recordsBulk.getRecords();
		LOG.debug("sortedRecordsList");
		for (Record record : sortedRecordsList) {
			LOG.debug(record.asString());
		}
	
		assertNotEquals(recordsList, sortedRecordsList);
		
		String[] sortedRecords = new String[] {
				"000,0",  
				"111,1",
				"222,2",
				"333,3"
			};
		
		LOG.debug("sortedRecordsList");
		recordsList = new ArrayList<>();
		for (int i = 0; i < sortedRecords.length; i++) {
			Record record = new Record(sortedRecords[i]);
			LOG.debug(record.toString());
			recordsList.add(record);
		}
		
		assertEquals(recordsList, sortedRecordsList);
	}

}
