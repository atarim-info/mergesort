/**
 * 
 */
package info.atarim.mergesort.csv;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import info.atarim.mergesort.Record;
import info.atarim.mergesort.concurrent.ThreadPool;

/**
 * @author vladimir
 *
 */
public class CSVFilesMerger {
	int indexPosition;
	int recordsLimit1;
	int recordsLimit2;
	private CSVReader csvReader1;
	private CSVReader csvReader2;

	/**
	 * @param csvReader1
	 * @param csvReader2
	 * @param csvWriter     TODO
	 * @param indexPosition
	 * @param recordsLimit
	 */
	private CSVFilesMerger(CSVReader csvReader1, CSVReader csvReader2, int indexPosition, int recordsLimit) {
		this.csvReader1 = csvReader1;
		this.csvReader2 = csvReader2;
		this.indexPosition = indexPosition;
		this.recordsLimit1 = recordsLimit / 2;
		this.recordsLimit2 = recordsLimit1 + (recordsLimit % 2);

	}

	public static CSVFilesMerger createMerger(String fileName1, String fileName2, int indexPosition, int recordsLimit) throws IOException {
		CSVReader csvReader1 = CSVReader.createReader(fileName1);
		CSVReader csvReader2 = CSVReader.createReader(fileName2);

		CSVFilesMerger merger = new CSVFilesMerger(csvReader1, csvReader2, indexPosition, recordsLimit);
		return merger;
	}

	public void mergeFiles(String mergeFileName, ThreadPool threadPool) throws IOException {
		// TODO Multithreaded

		final Thread parentThread = Thread.currentThread();
		threadPool.runInThread(parentThread, new Runnable() {
			@Override
			public void run() {
				CSVFilesMergerRegistry mergerRegistry = CSVFilesMergerRegistry.getInstance();
				Path pathToFile1 = csvReader1.getPathToFile();
				Path pathToFile2 = csvReader2.getPathToFile();
				mergerRegistry.waitForFilesBeReady(pathToFile1, pathToFile2);
				try {
					mergeFlesImpl(mergeFileName);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}, "MergeFiles to " + mergeFileName);
	}

	/**
	 * @param mergeFileName
	 * @throws IOException
	 */
	private void mergeFlesImpl(String mergeFileName) throws IOException {
		
		CSVWriter csvWriter = CSVWriter.createWriter(mergeFileName);
		
		Queue<Record> recordsQueue1 = new LinkedBlockingQueue<>();
		Queue<Record> recordsQueue2 = new LinkedBlockingQueue<>();

		Record record1 = pollRecords(csvReader1, recordsLimit1, recordsQueue1);
		Record record2 = pollRecords(csvReader2, recordsLimit2, recordsQueue2);
		while (record1 != null || record2 != null) {
			if (record1 != null) {
				if (record2 != null) {
					if (compare(record1, record2) > 0) {
						record2 = writeRecordAndPollNext(csvWriter, record2, csvReader2, recordsLimit2, recordsQueue2);

					} else {
						record1 = writeRecordAndPollNext(csvWriter, record1, csvReader1, recordsLimit1, recordsQueue1);
					}
				} else {
					record1 = writeRecordAndPollNext(csvWriter, record1, csvReader1, recordsLimit1, recordsQueue1);
				}
			} else if (record2 != null) {
				record2 = writeRecordAndPollNext(csvWriter, record2, csvReader2, recordsLimit2, recordsQueue2);
			}
		}
		csvWriter.close();
	}

	private Record writeRecordAndPollNext(CSVWriter csvWriter, Record record, CSVReader csvReader, int recordsLimit,
			Queue<Record> recordsQueue) throws IOException {
		csvWriter.writeRecord(record);
		record = pollRecords(csvReader, recordsLimit, recordsQueue);
		return record;
	}

	/**
	 * @param csvReader     TODO
	 * @param records1
	 * @param recordsQueue1
	 * @param record1
	 * @return
	 * @throws IOException
	 */
	private Record pollRecords(CSVReader csvReader, int recordsLimit, Queue<Record> recordsQueue) throws IOException {
		Record record = recordsQueue.poll();
		if (record == null && !csvReader.isEof()) {
			List<Record> records = csvReader.readRecordsFromCSV(recordsLimit);
			recordsQueue.addAll(records);
			record = recordsQueue.poll();
		}
		return record;
	}

	private int compare(Record o1, Record o2) {
		String s1 = o1.getRecord().get(indexPosition);
		String s2 = o2.getRecord().get(indexPosition);
		return s1.compareTo(s2);
	}

}
