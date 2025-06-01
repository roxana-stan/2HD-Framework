package scheduling_evaluation;

import java.text.DecimalFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javafx.util.Pair;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;

public class DagSchedulingMetrics {

	public enum DagMetricType {
		MAKESPAN,
		UTILITY,
		UTILITY_TIME_10,
		UTILITY_TIME_20,
		UTILITY_TIME_30,
		UTILITY_TIME_40,
		UTILITY_TIME_50,
		UTILITY_TIME_60,
		UTILITY_TIME_70,
		UTILITY_TIME_75,
		UTILITY_TIME_80,
		UTILITY_TIME_85,
		UTILITY_TIME_90,
		UTILITY_TIME_95,
		SCHEDULING_TIME;
	}

	private static Duration schedulingTimeDuration = Duration.ZERO;

	public static Map<DagMetricType, Double> collectMetrics(List<Cloudlet> cloudletList, TaskGraph taskGraph, boolean displayMetrics) {
		double makespan = SchedulingMetrics.computeMakespan(cloudletList);
		double utility = computeUtility(cloudletList, taskGraph, makespan);
		double utilityTime10 = computeUtilityTime(cloudletList, taskGraph, 0.10);
		double utilityTime20 = computeUtilityTime(cloudletList, taskGraph, 0.20);
		double utilityTime30 = computeUtilityTime(cloudletList, taskGraph, 0.30);
		double utilityTime40 = computeUtilityTime(cloudletList, taskGraph, 0.40);
		double utilityTime50 = computeUtilityTime(cloudletList, taskGraph, 0.50);
		double utilityTime60 = computeUtilityTime(cloudletList, taskGraph, 0.60);
		double utilityTime70 = computeUtilityTime(cloudletList, taskGraph, 0.70);
		double utilityTime75 = computeUtilityTime(cloudletList, taskGraph, 0.75);
		double utilityTime80 = computeUtilityTime(cloudletList, taskGraph, 0.80);
		double utilityTime85 = computeUtilityTime(cloudletList, taskGraph, 0.85);
		double utilityTime90 = computeUtilityTime(cloudletList, taskGraph, 0.90);
		double utilityTime95 = computeUtilityTime(cloudletList, taskGraph, 0.95);
		double schedulingTime = computeSchedulingTime();

		if (displayMetrics) {
			String indent = "    ";
			DecimalFormat dft = new DecimalFormat("###.##");

			Log.printLine("========== Evaluation Metrics ==========");
			Log.printLine("> Makespan:               " + indent + dft.format(makespan));
			Log.printLine("> Utility:                " + indent + dft.format(utility));
			Log.printLine("> @10 Utility Time:       " + indent + dft.format(utilityTime10));
			Log.printLine("> @20 Utility Time:       " + indent + dft.format(utilityTime20));
			Log.printLine("> @30 Utility Time:       " + indent + dft.format(utilityTime30));
			Log.printLine("> @40 Utility Time:       " + indent + dft.format(utilityTime40));
			Log.printLine("> @50 Utility Time:       " + indent + dft.format(utilityTime50));
			Log.printLine("> @60 Utility Time:       " + indent + dft.format(utilityTime60));
			Log.printLine("> @70 Utility Time:       " + indent + dft.format(utilityTime70));
			Log.printLine("> @75 Utility Time:       " + indent + dft.format(utilityTime75));
			Log.printLine("> @80 Utility Time:       " + indent + dft.format(utilityTime80));
			Log.printLine("> @85 Utility Time:       " + indent + dft.format(utilityTime85));
			Log.printLine("> @90 Utility Time:       " + indent + dft.format(utilityTime90));
			Log.printLine("> @95 Utility Time:       " + indent + dft.format(utilityTime95));
			Log.printLine("> Scheduling Time:        " + indent + schedulingTime);
		}

		Map<DagMetricType, Double> metrics = new HashMap<DagMetricType, Double>();
		metrics.put(DagMetricType.MAKESPAN, makespan);
		metrics.put(DagMetricType.UTILITY, utility);
		metrics.put(DagMetricType.UTILITY_TIME_10, utilityTime10);
		metrics.put(DagMetricType.UTILITY_TIME_20, utilityTime20);
		metrics.put(DagMetricType.UTILITY_TIME_30, utilityTime30);
		metrics.put(DagMetricType.UTILITY_TIME_40, utilityTime40);
		metrics.put(DagMetricType.UTILITY_TIME_50, utilityTime50);
		metrics.put(DagMetricType.UTILITY_TIME_60, utilityTime60);
		metrics.put(DagMetricType.UTILITY_TIME_70, utilityTime70);
		metrics.put(DagMetricType.UTILITY_TIME_75, utilityTime75);
		metrics.put(DagMetricType.UTILITY_TIME_80, utilityTime80);
		metrics.put(DagMetricType.UTILITY_TIME_85, utilityTime85);
		metrics.put(DagMetricType.UTILITY_TIME_90, utilityTime90);
		metrics.put(DagMetricType.UTILITY_TIME_95, utilityTime95);
		metrics.put(DagMetricType.SCHEDULING_TIME, schedulingTime);

		return metrics;
	}

	private static double computeUtility(List<Cloudlet> cloudletList, TaskGraph taskGraph, double makespan) {
		double utilitySum = 0.0;

		for (Cloudlet cloudlet : cloudletList) {
			if (cloudlet.getCloudletStatus() != Cloudlet.SUCCESS) {
				return Constants.INVALID_RESULT_DOUBLE;
			}

			Integer task = cloudlet.getCloudletId();
			double utility = taskGraph.computeTaskOutputData(task) * (makespan - SchedulingMetrics.cloudletCompletionTime(cloudlet));

			utilitySum += utility;
		}

		return utilitySum / makespan;
	}

	private static double computeUtilityTime(List<Cloudlet> cloudletList, TaskGraph taskGraph, double utilityPercentage) {
		double totalOutputData = 0.0;
		List<Pair<Double, Double>> taskUtilityInfo = new ArrayList<Pair<Double, Double>>(cloudletList.size());

		for (Cloudlet cloudlet : cloudletList) {
			if (cloudlet.getCloudletStatus() != Cloudlet.SUCCESS) {
				return Constants.INVALID_RESULT_DOUBLE;
			}

			Integer task = cloudlet.getCloudletId();
			double taskCompletionTime = SchedulingMetrics.cloudletCompletionTime(cloudlet);
			double taskOutputData = taskGraph.computeTaskOutputData(task);

			totalOutputData += taskOutputData;
			taskUtilityInfo.add(new Pair<Double, Double>(taskCompletionTime, taskOutputData));
		}

		if (totalOutputData == 0.0) {
			return 0.0;
		}

		// Sort the tasks by completion time.
		taskUtilityInfo.sort(Comparator.comparingDouble(Pair::getKey));

		double targetOutputData = utilityPercentage * totalOutputData;
		double accumulatedOutputData = 0.0;

		double utilityTime = 0.0;

		// Find the earliest time when the target output data is achieved.
		for (Pair<Double, Double> taskInfo : taskUtilityInfo) {
			double taskCompletionTime = taskInfo.getKey();
			double taskOutputData = taskInfo.getValue();

			accumulatedOutputData += taskOutputData;
			if (accumulatedOutputData >= targetOutputData) {
				utilityTime = taskCompletionTime;
				break;
			}
		}

		return utilityTime;
	}

	private static double computeSchedulingTime() {
		double schedulingTime = getTime(schedulingTimeDuration);
		return schedulingTime;
	}

	public static Duration getSchedulingTimeDuration() {
		return schedulingTimeDuration;
	}

	public static void setSchedulingTimeDuration(Duration timeDuration) {
		schedulingTimeDuration = timeDuration;
	}

	public static double getTime(Duration timeDuration) {
		double time = timeDuration.getSeconds() + timeDuration.getNano() / Math.pow(10, 9);
		return time;
	}

}
