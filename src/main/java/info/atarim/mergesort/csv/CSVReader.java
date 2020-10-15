package info.atarim.mergesort.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import info.atarim.mergesort.Record;

/**
 * Simple Java program to read CSV file in Java. In this program we will read
 * list of books stored in CSV file as comma separated values. * * @author
 * WINDOWS 8 *
 */
public class CSVReader {
	private final Path pathToFile;
	private BufferedReader br;
	private boolean eof = false;
	
	/**
	 * @param pathToFile
	 */
	private CSVReader(Path pathToFile) {
		this.pathToFile = pathToFile;
	}

	/**
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public static CSVReader createReader(String fileName) throws IOException {
		Path pathToFile = Paths.get(fileName);
		CSVReader CSVReader = new CSVReader(pathToFile); 
		return CSVReader;
	}
	
	/**
	 * @return
	 * @throws IOException
	 */
	private BufferedReader getBufferdReader() throws IOException {
		if (br == null) {
			br = Files.newBufferedReader(pathToFile, StandardCharsets.US_ASCII);
		}
		return br;
	}
	
	/**
	 * @param recordsLimit
	 * @return
	 * @throws IOException
	 */
	public List<Record> readRecordsFromCSV(int recordsLimit) throws IOException {
		List<Record> records = new ArrayList<>();

		// create an instance of BufferedReader
		// using try with resource, Java 7 feature to close resources

		// read the first line from the text file
		BufferedReader br = getBufferdReader();
		String line = br.readLine();
		
		// loop until all lines are read
		int recLimit = recordsLimit;
		while (line != null) {
			// use string.split to load a string array with the values from
			// each line of
			// the file, using a comma as the delimiter
			Record Record = new Record(line);
			// adding Record into ArrayList
			records.add(Record);
			recLimit--;
			if (recLimit == 0) {
				break;
			}
			// read next line before looping
			// if end of file reached, line would be null
			line = br.readLine();		
		}
		eof = (line == null);
		return records;
	}

	/**
	 * @return
	 */
	public boolean isEof() {
		return eof;
	}

	/**
	 * @return
	 */
	public Path getPathToFile() {
		return pathToFile;
	}

}
