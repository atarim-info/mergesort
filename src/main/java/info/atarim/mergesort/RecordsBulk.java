package info.atarim.mergesort;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class RecordsBulk {
	private final List<Record> records = new ArrayList<>();
	
	public RecordsBulk() {
	}

	public RecordsBulk(List<Record> records) {
		this.records.addAll(records);
	}

	public void addRecord(Record record) {
		records.add(record);
	}
	
	public void sort(final int indexPos) {
		records.sort(new Comparator<Record>() {

			@Override
			public int compare(Record o1, Record o2) {
				String s1 = o1.getRecord().get(indexPos);
				String s2 = o2.getRecord().get(indexPos);
				return s1.compareTo(s2);
			}
		});
		
	}

	public List<Record> getRecords() {
		List<Record> records = new ArrayList<>();
		records.addAll(this.records);
		return records;
	}
	
	

}
