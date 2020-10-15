/**
 * 
 */
package info.atarim.mergesort.csv;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * @author vladimir
 *
 */
public class CSVFilesMergerRegistry {
	private static Logger logger = Logger.getLogger("MergeSort");

	private static CSVFilesMergerRegistry thisInstance;
	
	private final Map<Path, Boolean> fileReadyMap;
	
	
	/**
	 * 
	 */
	private CSVFilesMergerRegistry() {
		fileReadyMap = new HashMap<>();
	}
	
	public static CSVFilesMergerRegistry getInstance() {
		if (thisInstance == null) {
			thisInstance = new CSVFilesMergerRegistry();
		}
		return thisInstance;
	}

	public int size() {
		return fileReadyMap.size();
	}

	public boolean isEmpty() {
		return fileReadyMap.isEmpty();
	}

	public boolean containsKey(Path key) {
		return fileReadyMap.containsKey(key);
	}

	public Boolean put(Path key, Boolean value) {
		return fileReadyMap.put(key, value);
	}

	public Boolean remove(Object key) {
		return fileReadyMap.remove(key);
	}

	public boolean replace(Path key, Boolean oldValue, Boolean newValue) {
		return fileReadyMap.replace(key, oldValue, newValue);
	}

	public Boolean replace(Path key, Boolean value) {
		return fileReadyMap.replace(key, value);
	}

	public Boolean get(Path key) {
		return fileReadyMap.get(key);
	}
	
	public boolean isFileRady(Path fileName) {
		Boolean fileReady = fileReadyMap.get(fileName);
		boolean fileOk = fileReady != null ? fileReady : false;
		return fileOk;
	}
	
	public boolean isFilesRady(Path fileName1, Path fileName2) {
		Boolean file1Ready = fileReadyMap.get(fileName1);
		Boolean file2Ready = fileReadyMap.get(fileName2);
		
		boolean file1Ok = file1Ready != null ? file1Ready : false;
		boolean file2Ok = file2Ready != null ? file2Ready : false;
		return file1Ok && file2Ok;
	}

	public void waitForFileBeReady(Path pathToFile) {
		logger.info("Waiting to file " + pathToFile);
		long waitTime = 0;
		while (!isFileRady(pathToFile)) {
			try {
				Thread.sleep(100);
				waitTime += 100;
			} 
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		logger.info("Waited " + waitTime + " ms to file " + pathToFile);
	}

	/**
	 * @param pathToFile1 
	 * @param pathToFile2 
	 */
	void waitForFilesBeReady(Path pathToFile1, Path pathToFile2) {
		logger.info("Waiting to files " + pathToFile1 + ", " + pathToFile2);
		long waitTime = 0;
		while (!isFilesRady(pathToFile1, pathToFile2)) {
			try {
				Thread.sleep(100);
				waitTime += 100;
			} 
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		logger.info("Waited " + waitTime + " ms to files " + pathToFile1 + ", " + pathToFile2);
	}
	
}
