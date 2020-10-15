package info.atarim.mergesort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class TestRecordsBulk {

	@Test
	public void test() {
		String[] records = new String[] {
			"111,1",
			"333,3",
			"222,2",
			"000,0"
		};
		
		System.out.println("recordsList");
		RecordsBulk recordsBulk = new RecordsBulk();
		for (int i = 0; i < records.length; i++) {
			Record record = new Record(records[i]);
			System.out.println(record);
			recordsBulk.addRecord(record);
			
		}
		List<Record> recordsList = recordsBulk.getRecords();
		recordsBulk.sort(1);
		
		List<Record> sortedRecordsList = recordsBulk.getRecords();
		System.out.println("sortedRecordsList");
		for (Record record : sortedRecordsList) {
			System.out.println(record);
		}
	
		assertNotEquals(recordsList, sortedRecordsList);
		
		String[] sortedRecords = new String[] {
				"000,0",  
				"111,1",
				"222,2",
				"333,3"
			};
		
		System.out.println("sortedRecordsList");
		recordsList = new ArrayList<>();
		for (int i = 0; i < sortedRecords.length; i++) {
			Record record = new Record(sortedRecords[i]);
			System.out.println(record);
			recordsList.add(record);
		}
		
		assertEquals(recordsList, sortedRecordsList);
	}

}
