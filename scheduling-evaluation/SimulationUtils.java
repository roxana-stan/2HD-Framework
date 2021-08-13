package scheduling_evaluation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Scanner;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.edge.core.edge.EdgeDataCenter;
import org.edge.core.edge.EdgeDevice;
import org.edge.core.edge.MicroELement;
import org.edge.core.feature.EdgeType;

import scheduling_evaluation.Types.ResourceType;

public class SimulationUtils {

	public static void printTasks(List<? extends Cloudlet> taskList) {		
		String indent = "    ";

		DecimalFormat dft = new DecimalFormat("###.##");
		dft.setMinimumIntegerDigits(2);

		Log.printLine("========== Tasks ==========");
		Log.printLine("Task ID" + indent + "Task Type" + indent + "Status" + indent
				+ "Datacenter ID" + indent + "VM ID" + indent
				+ "Time" + indent + "Start Time" + indent + "Finish Time");

		int taskCount = taskList.size();
		for (int idx = 0; idx < taskCount; idx++) {
			Task task = (Task) taskList.get(idx);

			Log.print(indent + dft.format(task.getCloudletId())
					+ indent + indent + task.getType()
					+ indent + indent + Cloudlet.getStatusString(task.getCloudletStatus()));

			if (task.getCloudletStatus() == Cloudlet.SUCCESS) {
				Log.printLine(indent + indent + dft.format(task.getResourceId())
							+ indent + indent + dft.format(task.getVmId())
							+ indent + indent + dft.format(SchedulingMetrics.cloudletExecutionTime(task))
							+ indent + indent + dft.format(task.getExecStartTime())
							+ indent + indent + dft.format(SchedulingMetrics.cloudletCompletionTime(task)));
			} else {
				Log.printLine();
			}
		}
	}


	public static void printEdgeDevices(Datacenter datacenter) {
		if (!(datacenter instanceof EdgeDataCenter)) {
			Log.printLine("Datacenter " + datacenter.getName() + " is not an edge datacenter.");
			return;
		}

		Log.printLine("========== " + datacenter.getName()
					+ " (" + SimulationUtils.getSupportedResourceType(datacenter) + ") ==========");
		for (Host machine : datacenter.getHostList()) {
			if (!(machine instanceof EdgeDevice)) {
				continue;
			}

			EdgeDevice edgeDevice = (EdgeDevice) machine;
			Log.printLine("EdgeDevice #" + edgeDevice.getId()
						+ " - consumed energy: " + (edgeDevice.getMaxBatteryCapacity() - edgeDevice.getCurrentBatteryCapacity()));
		}
	}


	public static ResourceType getSupportedResourceType(Datacenter datacenter) {
		if (datacenter instanceof EdgeDataCenter) {
			/* Assumptions:
			 * All edge devices of an edge datacenter have the same type.
			 * Edge datacenter contains at least one edge device.
			 */
			EdgeDevice edgeDevice = (EdgeDevice) datacenter.getHostList().get(0);
			if (edgeDevice.getType() == EdgeType.RASPBERRY_PI) {
				return ResourceType.EDGE_RESOURCE_RASPBERRY_PI;
			} else if (edgeDevice.getType() == EdgeType.MOBILE_PHONE) {
				return ResourceType.EDGE_RESOURCE_MOBILE_PHONE;
			} else {
				return null;
			}
		} else {
			return ResourceType.CLOUD_RESOURCE;
		}
	}


	public static ResourceType getResourceType(Vm vm) {
		if (vm instanceof MicroELement) {
			MicroELement mel = (MicroELement) vm;
			if (mel.getType() == EdgeType.RASPBERRY_PI) {
				return ResourceType.EDGE_RESOURCE_RASPBERRY_PI;
			} else if (mel.getType() == EdgeType.MOBILE_PHONE) {
				return ResourceType.EDGE_RESOURCE_MOBILE_PHONE;
			} else {
				return null;
			}
		} else {
			return ResourceType.CLOUD_RESOURCE;
		}
	}


	public static void generateTaskArrivalTimes(int taskCount) {
		try {
			String filename = getTaskArrivalTimesFile(taskCount);

			DecimalFormat dft = new DecimalFormat("#.##");

			File fout = new File(filename);
			FileOutputStream fos = new FileOutputStream(fout);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

			for (int idx = 0; idx < taskCount; ++idx) {
				double arrivalTime = getNewTaskArrivalTime();
				bw.write(dft.format(arrivalTime));
				bw.newLine();
			}

			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static double getNewTaskArrivalTime() {
		return -Math.log(1.0 - Math.random()) / Constants.ARRIVAL_RATE;
	}

	public static double[] loadTaskArrivalTimes(int taskCount) {
		double[] arrivalTimes = new double[taskCount];

		try {
			String filename = getTaskArrivalTimesFile(taskCount);

			Scanner scanner = new Scanner(new File(filename));

			int idx = 0;
			while (scanner.hasNextLine()) {
				double arrivalTime = Double.parseDouble(scanner.nextLine());
				arrivalTimes[idx++] = arrivalTime;
			}

			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		return arrivalTimes;
	}

	public static String getTaskArrivalTimesFile(int taskCount) {
		switch (taskCount) {
		case 32:
			return "ArrivalTimes_32Tasks.txt";
		case 80:
			return "ArrivalTimes_80Tasks.txt";
		case 160:
			return "ArrivalTimes_160Tasks.txt";
		case 800:
			return "ArrivalTimes_800Tasks.txt";
		case 1000:
			return "ArrivalTimes_1000Tasks.txt";
		case 2000:
			return "ArrivalTimes_2000Tasks.txt";
		case 4000:
			return "ArrivalTimes_4000Tasks.txt";
		case 8000:
			return "ArrivalTimes_8000Tasks.txt";
		}

		return null;
	}

}
