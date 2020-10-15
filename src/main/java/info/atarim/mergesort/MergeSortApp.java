package info.atarim.mergesort;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Logger;

import info.atarim.mergesort.concurrent.ThreadPool;
import info.atarim.mergesort.csv.CSVFilesMerger;
import info.atarim.mergesort.csv.CSVFilesMergerRegistry;
import info.atarim.mergesort.csv.CSVReader;
import info.atarim.mergesort.csv.CSVWriter;

public class MergeSortApp {
	private static Logger logger = Logger.getLogger("MergeSort"); 
	
	private final String fileName;
	private final int indexPosition;
	private final int recordsLimit;

	private ThreadPool threadPool;

	public MergeSortApp(String fileName, int indexPosition, int recordsLimit, int threadCount) {
		this.fileName = fileName;
		this.indexPosition = indexPosition;
		this.recordsLimit = recordsLimit;
		this.threadPool = ThreadPool.createThreadPoll(threadCount);
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args.length != 4) {
			System.out.println("Run with params: <fileName> <indexPosition> <recordsLimit> <threadCount>");
		}

		String fileName = args[0];
		int indexPosition = Integer.parseInt(args[1]);
		int recordsLimit = Integer.parseInt(args[2]);
		int threadCount = Integer.parseInt(args[3]);
		
		System.out.println("Params: fileName = " + fileName +  
						   ", indexPosition = " + indexPosition + 
						   ", recordsLimit = " + recordsLimit +
						   ", threadCount = " + threadCount);
		MergeSortApp sortApp = new MergeSortApp(fileName, indexPosition, recordsLimit, threadCount);
		try {
			sortApp.readSplitAndSortFile(fileName, indexPosition, recordsLimit, threadCount);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @param fileName
	 * @param indexPosition
	 * @param recordsLimit
	 * @param threadCount
	 * @throws IOException
	 */
	private void readSplitAndSortFile(String fileName, int indexPosition, int recordsLimit, int threadCount) throws IOException {
		CSVReader csvReader = CSVReader.createReader(fileName);
		int iteration = 0;
	
		int filesCount = readRecords(csvReader, recordsLimit, indexPosition, ++iteration);
		
		String resultFileName = mergeFiles(indexPosition, recordsLimit, filesCount, ++iteration);
		
		String outputFileName = fileName.replace(".", "_sorted.");
		renameFile(resultFileName, outputFileName);
		System.out.println("Finished sortning file: " + fileName);
		writeFileNameToConsole(outputFileName);		
	}


	/**
	 * @param br
	 * @param recordsLimit
	 * @param indexPosition
	 * @throws IOException
	 */
	private int readRecords(CSVReader csvReader, int recordsLimit, final int indexPosition, int iteration) throws IOException {
		List<Record> records = csvReader.readRecordsFromCSV(recordsLimit);
		int counter = 0;
		while (!records.isEmpty()) {
			counter ++;
			RecordsBulk recordsBulk = new RecordsBulk(records);
			sortAndWriteRecords(indexPosition, recordsBulk, counter, iteration);
			records = csvReader.readRecordsFromCSV(recordsLimit);
		}
		System.out.println("Written files: " + counter);
		return counter;
	}

	/**
	 * @param indexPosition
	 * @param recordsLimit
	 * @param filesCount
	 * @param iteration TODO
	 * @throws IOException
	 */
	private String mergeFiles(int indexPosition, int recordsLimit, int filesCount, int iteration) throws IOException {
		int counter = 0;
		int reviouseIteration = iteration - 1;
		String mergedFileName = null;
		
		for (int i = 1; i <= filesCount; i += 2) {
			String fileName1 = createTempFileName(reviouseIteration, i);	
			if (filesCount > i) {
				mergedFileName = createTempFileName(iteration, ++counter);
				String fileName2 = createTempFileName(reviouseIteration, i + 1);
				CSVFilesMerger filesMerger = CSVFilesMerger.createMerger(fileName1, fileName2, indexPosition, recordsLimit);
				writeFileNameToConsole(mergedFileName);
				filesMerger.mergeFiles(mergedFileName, threadPool);
			}
			else {
				mergedFileName = createTempFileName(iteration, ++counter);
				renameFile(fileName1, mergedFileName);
			}			
		}
		if (counter > 1) {
			//TODO multi-threaded
			mergedFileName = mergeFiles(indexPosition, recordsLimit, counter, ++iteration);
		}
		return mergedFileName;
	}
	
	
	/**
	 * @param fileName
	 * @param newFileName
	 */
	private void renameFile(String fileName, String newFileName) {
		
		final Thread parentThread = Thread.currentThread();
		threadPool.runInThread(parentThread, new Runnable() {
			@Override
			public void run() {
				renameFileImpl(fileName, newFileName);
			}

		}, "Rename to File " + newFileName);
	}

	/**
	 * @param fileName
	 * @param newFileName
	 */
	private void renameFileImpl(String fileName, String newFileName) {
		logger.info("Rename File " + fileName + " to " + newFileName);
		Path pathToFile = Paths.get(fileName);	
		Path pathToNewFile = Paths.get(newFileName);
		
		CSVFilesMergerRegistry mergerRegistry = CSVFilesMergerRegistry.getInstance();
		mergerRegistry.put(pathToNewFile, false);		
		mergerRegistry.waitForFileBeReady(pathToFile);
		
		File file = pathToFile.toFile();
		file.renameTo(pathToNewFile.toFile());
		
		mergerRegistry.replace(pathToNewFile, false, true);
	}

	/**
	 * @param indexPosition
	 * @param recordsBulk
	 * @param iteration TODO
	 */
	private void sortAndWriteRecords(final int indexPosition, RecordsBulk recordsBulk, int counter, int iteration) {
		final Thread parentThread = Thread.currentThread();
		threadPool.runInThread(parentThread, new Runnable() {
			@Override
			public void run() {
				sortAndWriteRecordsImpl(recordsBulk, indexPosition, counter, iteration);
			}
		}, "SortRecords " + counter);
	}

	private void sortAndWriteRecordsImpl(RecordsBulk recordsBulk, final int indexPosition, int counter,	int iteration) {
		recordsBulk.sort(indexPosition);
		try {
			String sortedfileName = createTempFileName(iteration, counter);
			writeFileNameToConsole(sortedfileName);
			CSVWriter csvWriter = CSVWriter.createWriter(sortedfileName);
			csvWriter.writeRecordsToCSVFile(recordsBulk.getRecords());
			csvWriter.close(); 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void writeFileNameToConsole(String sortedfileName) {
		System.out.println("Write file: " + sortedfileName);
	}

	/**
	 * @param counter
	 * @return
	 */
	private String createTempFileName(int iteration, int counter) {
		return fileName + "." + iteration + "." + counter + ".tmp";
	}
}