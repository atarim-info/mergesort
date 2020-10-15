package info.atarim.mergesort.csv;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import info.atarim.mergesort.Record;

public class CSVWriter {
	private final Path pathToFile;
	private BufferedWriter bw;

	/**
	 * @param bw
	 */
	private CSVWriter(Path pathToFile) {
		this.pathToFile = pathToFile;

		CSVFilesMergerRegistry mergerRegistry = CSVFilesMergerRegistry.getInstance();
		mergerRegistry.put(pathToFile, false);
	}

	public static  CSVWriter createWriter(String fileName) throws IOException {
		Path pathToFile = Paths.get(fileName);
		CSVWriter csvWriter = new CSVWriter( pathToFile);
		return csvWriter;
	}
	
	public BufferedWriter getBufferedWriter() throws IOException {
		if (bw == null) {
			bw = Files.newBufferedWriter(pathToFile, StandardCharsets.US_ASCII);
		}
		return bw;
	}
	
	public void writeRecordsToCSVFile(List<Record> records) throws IOException {
		for (Record record : records) {
			writeRecord(record);
		}
	}

	/**
	 * @param record
	 * @throws IOException
	 */
	public void writeRecord(Record record) throws IOException {
		BufferedWriter bw = getBufferedWriter();
		String row = record.asString();
		bw.write(row);
		bw.newLine();
	}
	
	public Path getPathToFile() {
		return pathToFile;
	}
	
	public void flush() throws IOException {
		bw.flush();
	}

	public void close() throws IOException {
		bw.close();
		
		CSVFilesMergerRegistry mergerRegistry = CSVFilesMergerRegistry.getInstance();
		mergerRegistry.replace(pathToFile, false, true);
		
		System.out.println("File " + pathToFile + " closed");
	}
	
}
