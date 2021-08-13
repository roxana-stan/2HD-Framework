package scheduling_evaluation;

import java.util.LinkedHashMap;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;

import scheduling_evaluation.Types.ResourceType;
import scheduling_evaluation.Types.TaskType;

public class TaskUtils {

	private static final double DOUBLE_THRESHOLD = 0.0001;

	/* ========== Task's expected execution times on a VM resource ========== */

	public static double getTotalExecutionTime(Cloudlet cloudlet, Vm vm) {
		return getCpuExecutionTime(cloudlet, vm) + getTransferTime(cloudlet, vm);
	}

	public static double getCpuExecutionTime(Cloudlet cloudlet, Vm vm) {
		return cloudlet.getCloudletLength() / vm.getMips();
	}

	public static double getTransferTime(Cloudlet cloudlet, Vm vm) {
		return getUploadTime(cloudlet, vm) + getDownloadTime(cloudlet, vm);
	}

	public static double getUploadTime(Cloudlet cloudlet, Vm vm) {
		if (Math.abs(cloudlet.getCloudletFileSize() - (long) (1.0)) < DOUBLE_THRESHOLD) {
			return 0.0;
		}
		return cloudlet.getCloudletFileSize() / vm.getBw();
	}

	public static double getDownloadTime(Cloudlet cloudlet, Vm vm) {
		if (Math.abs(cloudlet.getCloudletOutputSize() - (long) (1.0)) < DOUBLE_THRESHOLD) {
			return 0.0;
		}
		return cloudlet.getCloudletOutputSize() / vm.getBw();
	}

	public static void printExecutionTimes(Cloudlet cloudlet, Vm vm) {
		Task task = (Task) cloudlet;
		Log.printLine("Task " + task.getCloudletId() + ", type " + task.getType() + ", VM #" + vm.getId()
					+ " - Total execution time: " + getTotalExecutionTime(task, vm)
					+ " = Processing time: " + getCpuExecutionTime(task, vm)
					+ " + Transfer time: " + getTransferTime(task, vm));
	}

	/* ========== Update task's properties ========== */

	public static void setCloudletTotalLength(Cloudlet cloudlet, Vm vm) {
		double cloudletTotalExecutionTime = getTotalExecutionTime(cloudlet, vm);
		double cloudletTotalLength = cloudletTotalExecutionTime * vm.getMips();

		if (Math.abs(cloudletTotalLength - (long) (cloudletTotalLength)) >= DOUBLE_THRESHOLD) {
			Log.printLine("Cloudlet " + cloudlet.getCloudletId() + " - Total length: " + cloudletTotalLength
					+ " -> Please adjust experimental input data to avoid any time-related simulation issues.");
		}

		cloudlet.setCloudletLength((long) cloudletTotalLength);
	}

	/* ========== Task's energy consumption on a VM resource ========== */

	public static double getEnergyConsumption(Cloudlet cloudlet, Vm vm) {
		ResourceType resourceType = SimulationUtils.getResourceType(vm);
		if (resourceType == ResourceType.CLOUD_RESOURCE) {
			return 0.0;
		}

		double processingTime = getCpuExecutionTime(cloudlet, vm);
		double transferTime = getTransferTime(cloudlet, vm);

		double consumption = processingTime * 1.0;
		if (resourceType == ResourceType.EDGE_RESOURCE_MOBILE_PHONE) {
			consumption += transferTime * Constants.SMARTPHONE_PM_BATTERY_DRAINAGE_RATE;
		} else if (resourceType == ResourceType.EDGE_RESOURCE_RASPBERRY_PI) {
			consumption += transferTime * Constants.RASPBERRY_PI_PM_BATTERY_DRAINAGE_RATE;
		}

		return consumption;
	}

	/* ========== Task parameters ========== */

	public static double getTaskLength(TaskType type) {
		switch (type) {
		case RT1:
			return Constants.READ_TASK1_LENGTH;
		case RT2:
			return Constants.READ_TASK2_LENGTH;
		case RT3:
			return Constants.READ_TASK3_LENGTH;
		case RT4:
			return Constants.READ_TASK4_LENGTH;
		case WT1:
			return Constants.WRITE_TASK1_LENGTH;
		case WT2:
			return Constants.WRITE_TASK2_LENGTH;
		case WT3:
			return Constants.WRITE_TASK3_LENGTH;
		case WT4:
			return Constants.WRITE_TASK4_LENGTH;
		default:
			return 0.0;
		}
	}

	public static double getTaskFileSize(TaskType type) {
		switch (type) {
		case RT1:
			return Constants.READ_TASK1_FILESIZE;
		case RT2:
			return Constants.READ_TASK2_FILESIZE;
		case RT3:
			return Constants.READ_TASK3_FILESIZE;
		case RT4:
			return Constants.READ_TASK4_FILESIZE;
		case WT1:
			return Constants.WRITE_TASK1_FILESIZE;
		case WT2:
			return Constants.WRITE_TASK2_FILESIZE;
		case WT3:
			return Constants.WRITE_TASK3_FILESIZE;
		case WT4:
			return Constants.WRITE_TASK4_FILESIZE;
		default:
			return 0.0;
		}
	}

	public static double getTaskOutputSize(TaskType type) {
		switch (type) {
		case RT1:
			return Constants.READ_TASK1_OUTPUTSIZE;
		case RT2:
			return Constants.READ_TASK2_OUTPUTSIZE;
		case RT3:
			return Constants.READ_TASK3_OUTPUTSIZE;
		case RT4:
			return Constants.READ_TASK4_OUTPUTSIZE;
		case WT1:
			return Constants.WRITE_TASK1_OUTPUTSIZE;
		case WT2:
			return Constants.WRITE_TASK2_OUTPUTSIZE;
		case WT3:
			return Constants.WRITE_TASK3_OUTPUTSIZE;
		case WT4:
			return Constants.WRITE_TASK4_OUTPUTSIZE;
		default:
			return 0.0;
		}
	}

	public static Map<TaskType, Integer> getTaskDistribution() {
		Map<TaskType, Integer> taskDistribution = new LinkedHashMap<>();
		
		// Compute number of read and write tasks
		int t = Constants.TASK_COUNT;
		int r = (int) (Constants.F * t);
		int w = (int) ((1 - Constants.F) * t);
		if (r + w != t) {
			Log.printLine("Task count: " + t + " (Read: " + r + " Write: " + w + ")"
						+ " -> Please adjust experimental input data.");
			return null;
		}

		// Compute distribution of read tasks
		int r1 = (int) (Constants.F_RT1 * r);
		int r2 = (int) (Constants.F_RT2 * r);
		int r3 = (int) (Constants.F_RT3 * r);
		int r4 = (int) (Constants.F_RT4 * r);
		if (r1 + r2 + r3 + r4 != r) {
			Log.printLine("Read task count: " + r + " (#RT1: " + r1 + " #RT2: " + r2 + " #RT3: " + r3 + " #RT4: " + r4 +")"
						+ " -> Please adjust experimental input data.");
			return null;
		}
		taskDistribution.put(TaskType.RT1, r1);
		taskDistribution.put(TaskType.RT2, r2);
		taskDistribution.put(TaskType.RT3, r3);
		taskDistribution.put(TaskType.RT4, r4);
		
		// Compute distribution of write tasks
		int w1 = (int) (Constants.F_WT1 * w);
		int w2 = (int) (Constants.F_WT2 * w);
		int w3 = (int) (Constants.F_WT3 * w);
		int w4 = (int) (Constants.F_WT4 * w);
		if (w1 + w2 + w3 + w4 != w) {
			Log.printLine("Write task count: " + w + " (#WT1: " + w1 + " #WT2: " + w2 + " #WT3: " + w3 + " #WT4: " + w4 +")"
						+ " -> Please adjust experimental input data.");
			return null;
		}
		taskDistribution.put(TaskType.WT1, w1);
		taskDistribution.put(TaskType.WT2, w2);
		taskDistribution.put(TaskType.WT3, w3);
		taskDistribution.put(TaskType.WT4, w4);

		return taskDistribution;
	}

}
