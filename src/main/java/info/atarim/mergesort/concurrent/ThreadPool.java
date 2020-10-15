package info.atarim.mergesort.concurrent;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ThreadPool {
	private int threadCount;
	private Set<Thread> parentThreads = new HashSet<>();
	private Map<Thread, Thread> threadCollection = new HashMap<>();
	
	private static ThreadPool thisInstance;

	/**
	 * @param threadCount
	 */
	private ThreadPool(int threadCount) {
		this.threadCount = threadCount;
	}
	
	public static ThreadPool createThreadPoll(int threadCount) {
		if (thisInstance == null) {
			thisInstance = new ThreadPool(threadCount);
		}
		return thisInstance;
	}
	
	public Thread runInThread(Thread parentThread, Runnable runnable, String label) {
		Thread newThread;
		while (!checkAvilability()) {
			try {
				Thread.sleep(100);
			} 
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		newThread = createThread(parentThread, runnable, label);	
		newThread.start();
		return newThread;
	}

	/**
	 * @param parentThread
	 * @param runnable
	 * @return
	 */
	private synchronized Thread createThread(Thread parentThread, Runnable runnable, String label) {
		Thread newThread;
		newThread = new Thread(runnable, label);
		threadCollection.put(newThread, parentThread);
		parentThreads.add(parentThread);
		return newThread;
	}

	/**
	 * 
	 */
	private boolean checkAvilability() {
		Set<Thread> threadsToRemove = new HashSet<>();
		for (Thread thread : threadCollection.keySet()) {
			if (!thread.isAlive()) {
				//Thread parentThread2 = threadCollection.get(thread);
				threadsToRemove.add(thread);
			}
		}
		for (Thread thread : threadsToRemove) {
			threadCollection.remove(thread);
		}
		return threadCollection.size() < threadCount;
	}

}
