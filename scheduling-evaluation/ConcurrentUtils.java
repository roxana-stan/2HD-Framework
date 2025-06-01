package scheduling_evaluation;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.cloudbus.cloudsim.Log;

public class ConcurrentUtils {

	public static void stop(ExecutorService executor) {
		try {
			Log.printLine("Attempt to shut down executor");
			executor.shutdown();
			executor.awaitTermination(60, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Log.printLine("Tasks interrupted");
		} finally {
			if (!executor.isTerminated()) {
				Log.printLine("Killing non-finished tasks");
			}
			executor.shutdownNow();
			Log.printLine("Shutdown finished");
		}
	}

	public static void sleep(int seconds) {
		try {
			TimeUnit.SECONDS.sleep(seconds);
		} catch (InterruptedException e) {
			Log.printLine("Sleep interrupted");
		}
	}

}
