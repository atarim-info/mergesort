package info.atarim.mergesort;

import java.util.Arrays;
import java.util.List;

public class Record {
	private static final String DELIMERTER = ",";
	private List<String> record;

	
	public Record(String line) {
		createRecord(line);
	}


	private void createRecord(String line) {
		String[] attributes = line.split(DELIMERTER);
		setRecord(Arrays.asList(attributes));
	}


	public List<String> getRecord() {
		return record;
	}


	public void setRecord(List<String> record) {
		this.record = record;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((record == null) ? 0 : record.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Record other = (Record) obj;
		if (record == null) {
			if (other.record != null)
				return false;
		} 
		else {
			if (record.size() == other.record.size()) {
				for (int i = 0; i < record.size(); i++) {
					if (!record.get(i).equals(other.record.get(i))) {
						return false;
					}
				}
			}
			
		}
		return true;
	}


	@Override
	public String toString() {
		return "Record [record = " + record + "]";
	}
	
	/**
	 * @return
	 */
	public String asString( ) {
		StringBuilder builder = new StringBuilder();
		for (String string : record) {
			builder.append(string).append(DELIMERTER);
			
		} 
		return builder.substring(0, builder.length() - 1);
	}

}
