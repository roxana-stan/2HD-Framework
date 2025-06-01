package scheduling_evaluation;

import java.text.DecimalFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;

import scheduling_evaluation.Types.TaskExecutionResourceStatus;

public class SchedulingMetrics {

	public static final double INVALID_RESULT = -1.0;

	public static void collectMetrics(List<Cloudlet> cloudletList) {
		String indent = "    ";
		DecimalFormat dft = new DecimalFormat("###.##");

		Log.printLine("========== Evaluation Metrics ==========");
		Log.printLine("> Total Execution Time:   " + indent + dft.format(computeTotalExecutionTime(cloudletList)));
		Log.printLine("> Average Turnaround Time:" + indent + dft.format(computeAvgTurnaroundTime(cloudletList)));
		Log.printLine("> Average Waiting Time:   " + indent + dft.format(computeAvgWaitingTime(cloudletList)));

		Log.printLine("> Makespan:               " + indent + dft.format(computeMakespan(cloudletList)));
		Log.printLine("> Throughput:             " + indent + dft.format(computeThroughput(cloudletList)));
	}

	public static double computeResourceUtilization(List<Cloudlet> cloudletList, Datacenter datacenter) {
		int datacenterId = datacenter.getId();
		Log.printLine("========== " + datacenter.getName()
				+ " (" + SimulationUtils.getSupportedResourceType(datacenter) + ") ==========");

		Map<Integer, Double> vmExecTimes = new LinkedHashMap<>();
		for (Cloudlet cloudlet : cloudletList) {
			if (cloudlet.getResourceId() != datacenterId) {
				continue;
			}

			double execTime = cloudletExecutionTime(cloudlet);

			int vmId = cloudlet.getVmId();
			if (vmExecTimes.containsKey(vmId)) {
				execTime += vmExecTimes.get(vmId);
				vmExecTimes.remove(vmId);
			}
			vmExecTimes.put(vmId, execTime);
		}

		DecimalFormat dft = new DecimalFormat("##.##");
		double totalTime = computeMakespan(cloudletList);

		double allResourceUtilizations = 0.0;
		int activeHostCount = 0;

		List<Host> hostList = datacenter.getHostList();
		for (Host host : hostList) {
			if (host.getVmList().isEmpty()) {
				continue;
			}
			++activeHostCount;

			double hostResourceUtilization = 0.0;
			int vmId = host.getVmList().get(0).getId();
			if (vmExecTimes.containsKey(vmId)) {
				hostResourceUtilization = vmExecTimes.get(vmId);
			}
			Log.printLine("Physical Machine #" + host.getId() + ", VM #" + vmId
						+ " - " + dft.format(hostResourceUtilization) + " / " + dft.format(totalTime));

			allResourceUtilizations += (hostResourceUtilization / totalTime);
		}

		double resourceUtilization = 100.0 * allResourceUtilizations / activeHostCount;
		Log.printLine("Resource Utilization: " + dft.format(resourceUtilization) + "%");

		return resourceUtilization;
	}

	public static void printTaskFailureStatistics(List<Cloudlet> executedTasks, List<Cloudlet> notExecutedTasks) {
		DecimalFormat dft = new DecimalFormat("##.##");
		Log.printLine("========== Task Failure Statistics ==========");

		int failedTasks = notExecutedTasks.size();
		int tasks = executedTasks.size() + failedTasks;

		double failurePercentage = 100.0 * failedTasks / tasks;
		System.out.println("Failed tasks: " + dft.format(failurePercentage) + "% (" + failedTasks + " / " + tasks + ")");

		int limitedMemoryFailedTasks = 0;
		int drainedBatteryFailedTasks = 0;
		for (Cloudlet cloudlet : notExecutedTasks) {
			Task task = (Task) cloudlet;
			TaskExecutionResourceStatus resourceStatus = task.getResourceStatus();
			if (resourceStatus == TaskExecutionResourceStatus.FAILURE_EDGE_LIMITED_MEMORY) {
				limitedMemoryFailedTasks++;
			}
			if (resourceStatus == TaskExecutionResourceStatus.FAILURE_EDGE_DRAINED_BATTERY) {
				drainedBatteryFailedTasks++;
			}
		}

		double limitedMemoryFailurePercentage = 100.0 * limitedMemoryFailedTasks / tasks;
		double drainedBatteryFailurePercentage = 100.0 * drainedBatteryFailedTasks / tasks;
		System.out.println("-> Edge limited memory: " + dft.format(limitedMemoryFailurePercentage) + "% (" + limitedMemoryFailedTasks + " / " + tasks + ")");
		System.out.println("-> Edge drained battery: " + dft.format(drainedBatteryFailurePercentage) + "% (" + drainedBatteryFailedTasks + " / " + tasks + ")");
	}

	/* ========== Task scheduling metrics ========== */

	/**
	 * Computes total execution time of all tasks.
	 */
	public static double computeTotalExecutionTime(List<Cloudlet> cloudletList) {
		double totalExecutionTime = 0.0;

		for (Cloudlet cloudlet : cloudletList) {
			if (cloudlet.getCloudletStatus() != Cloudlet.SUCCESS) {
				return INVALID_RESULT;
			}
			totalExecutionTime += cloudletExecutionTime(cloudlet);
		}

		return totalExecutionTime;
	}

	/**
	 * Computes mean turnaround time / time in system across tasks.
	 */
	public static double computeAvgTurnaroundTime(List<Cloudlet> cloudletList) {
		double turnaroundTimeSum = 0.0;
		for (Cloudlet cloudlet : cloudletList) {
			if (cloudlet.getCloudletStatus() != Cloudlet.SUCCESS) {
				return INVALID_RESULT;
			}
			turnaroundTimeSum += cloudletTurnaroundTime(cloudlet);
		}

		double avgTurnaroundTime = turnaroundTimeSum / cloudletList.size();
		return avgTurnaroundTime;
	}

	/**
	 * Computes mean waiting time / queuing time across tasks.
	 */
	public static double computeAvgWaitingTime(List<Cloudlet> cloudletList) {
		double waitingTimeSum = 0.0;
		for (Cloudlet cloudlet : cloudletList) {
			if (cloudlet.getCloudletStatus() != Cloudlet.SUCCESS) {
				return INVALID_RESULT;
			}
			waitingTimeSum += cloudletWaitingTime(cloudlet);
		}

		double avgWaitingTime = waitingTimeSum / cloudletList.size();
		return avgWaitingTime;
	}

	/**
	 * Computes makespan: overall time taken to execute all tasks, from tasks' arrival until completion.
	 */
	public static double computeMakespan(List<Cloudlet> cloudletList) {
		double maxCompletionTime = INVALID_RESULT;
		double minArrivalTime = INVALID_RESULT;
		for (Cloudlet cloudlet : cloudletList) {
			if (cloudlet.getCloudletStatus() != Cloudlet.SUCCESS) {
				return INVALID_RESULT;
			}

			double completionTime = cloudletCompletionTime(cloudlet);
			if (maxCompletionTime == INVALID_RESULT) {
				maxCompletionTime = completionTime;
			} else {
				maxCompletionTime = Math.max(maxCompletionTime, completionTime);
			}

			double arrivalTime = cloudletArrivalTime(cloudlet);
			if (minArrivalTime == INVALID_RESULT) {
				minArrivalTime = arrivalTime;
			} else {
				minArrivalTime = Math.min(minArrivalTime, arrivalTime);
			}
		}

		double makespan = maxCompletionTime - minArrivalTime;
		return makespan;
	}

	/**
	 * Computes system throughput.
	 */
	public static double computeThroughput(List<Cloudlet> cloudletList) {
		double cloudletCount = cloudletList.size();

		double throughput = cloudletCount / computeMakespan(cloudletList);
		return throughput;
	}


	/* ========== Cloudlet time methods ========== */
	/* Note: The Cloudlet built-in methods consider only the latest used cloud resource.
	 * 		 Refer to the Cloudlet resList if needed.
	 */

	/**
	 * Arrival time: time moment when task enters the system and is ready for its execution.
	 */
	public static double cloudletArrivalTime(Cloudlet cloudlet) {
		return cloudlet.getSubmissionTime();
	}

	/**
	 * Completion time: time moment when task's execution completes and task exits the system.
	 */
	public static double cloudletCompletionTime(Cloudlet cloudlet) {
		return cloudlet.getFinishTime();
	}

	/**
	 * Execution time: required processing time to execute a task on a computation resource.
	 */
	public static double cloudletExecutionTime(Cloudlet cloudlet) {
		return cloudlet.getActualCPUTime();
	}

	/**
	 * Turnaround time: total time spent by the task in the system, from task's arrival until its completion.
	 */
	public static double cloudletTurnaroundTime(Cloudlet cloudlet) {
		// Turnaround time = Completion time - Arrival time
		return cloudletCompletionTime(cloudlet) - cloudletArrivalTime(cloudlet);	// cloudlet.getWallClockTime()
	}

	/**
	 * Waiting time: amount of time task is ready for its execution and waits.
	 */
	public static double cloudletWaitingTime(Cloudlet cloudlet) {
		// Waiting time = Turnaround time - Execution time
		return cloudletTurnaroundTime(cloudlet) - cloudletExecutionTime(cloudlet);	// cloudlet.getWaitingTime()
	}

}
