package scheduling_evaluation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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
		if (taskList.isEmpty()) {
			return;
		}

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
		/* Generate Poisson-based task arrival times. */
		// Generate inter-arrival times.
		double[] interArrivalTimes = new double[taskCount];
		for (int idx = 0; idx < taskCount; ++idx) {
			interArrivalTimes[idx] = getExponentialRandom(Constants.TASK_ARRIVAL_RATE);
		}
		// Calculate cumulative sum to get arrival times.
		double[] arrivalTimes = new double[taskCount];
		arrivalTimes[0] = interArrivalTimes[0];
		for (int idx = 1; idx < taskCount; ++idx) {
			arrivalTimes[idx] = arrivalTimes[idx-1] + interArrivalTimes[idx];
		}

		try {
			DecimalFormat dft = new DecimalFormat("#.##");

			String filename = getTaskArrivalTimesFile(taskCount);
			File fout = new File(filename);
			FileOutputStream fos = new FileOutputStream(fout);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

			for (int idx = 0; idx < taskCount; ++idx) {
				double arrivalTime = arrivalTimes[idx];
				bw.write(dft.format(arrivalTime));
				bw.newLine();
			}

			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static double[] loadTaskArrivalTimes(int taskCount) {
		double[] arrivalTimes = new double[taskCount];

		try {
			String filename = getTaskArrivalTimesFile(taskCount);
			Scanner scanner = new Scanner(new File(filename));

			for (int idx = 0; idx < taskCount; ++idx) {
				double arrivalTime = Double.parseDouble(scanner.nextLine());
				arrivalTimes[idx] = arrivalTime;
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
			return "data/times/task_arrival_times_32_tasks.txt";
		case 80:
			return "data/times/task_arrival_times_80_tasks.txt";
		case 160:
			return "data/times/task_arrival_times_160_tasks.txt";
		case 800:
			return "data/times/task_arrival_times_800_tasks.txt";
		case 1000:
			return "data/times/task_arrival_times_1000_tasks.txt";
		case 2000:
			return "data/times/task_arrival_times_2000_tasks.txt";
		case 4000:
			return "data/times/task_arrival_times_4000_tasks.txt";
		case 8000:
			return "data/times/task_arrival_times_8000_tasks.txt";
		}

		return null;
	}

	public static void generateTaskSubgraphArrivalTimes(int taskSubgraphCount) {
		/* Generate Poisson-based task subgraph arrival times. */
		// Generate inter-arrival times.
		double[] interArrivalTimes = new double[taskSubgraphCount];
		for (int idx = 0; idx < taskSubgraphCount; ++idx) {
			interArrivalTimes[idx] = getExponentialRandom(Constants.TASK_SUBGRAPH_ARRIVAL_RATE);
		}
		// Calculate cumulative sum to get arrival times.
		double[] arrivalTimes = new double[taskSubgraphCount];
		arrivalTimes[0] = interArrivalTimes[0];
		for (int idx = 1; idx < taskSubgraphCount; ++idx) {
			arrivalTimes[idx] = arrivalTimes[idx-1] + interArrivalTimes[idx];
		}

		try {
			DecimalFormat dft = new DecimalFormat("#.##");

			String filename = Constants.TASK_SUBGRAPH_ARRIVAL_TIMES_FILENAME;
			File fout = new File(filename);
			FileOutputStream fos = new FileOutputStream(fout);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

			for (int idx = 0; idx < taskSubgraphCount; ++idx) {
				double arrivalTime = (int) arrivalTimes[idx];
				bw.write(dft.format(arrivalTime));
				bw.newLine();
			}

			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static List<Double> loadTaskSubgraphArrivalTimes(int taskSubgraphCount) {
		List<Double> arrivalTimes = new ArrayList<Double>(taskSubgraphCount);

		try {
			String filename = Constants.TASK_SUBGRAPH_ARRIVAL_TIMES_FILENAME;
			Scanner scanner = new Scanner(new File(filename));

			for (int idx = 0; idx < taskSubgraphCount; ++idx) {
				double arrivalTime = Double.parseDouble(scanner.nextLine());
				arrivalTimes.add(arrivalTime);
			}

			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		return arrivalTimes;
	}

	public static void generateResourceAvailabilityTimes() {
		int resourceCountDefaultAvailability = Constants.RESOURCE_COUNT_DEFAULT_AVAILABILITY;
		int resourceCount = Constants.RESOURCE_COUNT - resourceCountDefaultAvailability;

		/* Generate Poisson-based resource availability times. */
		// Generate inter-availability times.
		double[] interAvailabilityTimes = new double[resourceCount];
		for (int idx = 0; idx < resourceCount; ++idx) {
			interAvailabilityTimes[idx] = getExponentialRandom(Constants.RESOURCE_AVAILABILITY_RATE);
		}
		// Calculate cumulative sum to get availability times.
		double[] availabilityTimes = new double[resourceCount];
		availabilityTimes[0] = interAvailabilityTimes[0];
		for (int idx = 1; idx < resourceCount; ++idx) {
			availabilityTimes[idx] = availabilityTimes[idx-1] + interAvailabilityTimes[idx];
		}

		try {
			DecimalFormat dft = new DecimalFormat("#.##");

			String filename = Constants.RESOURCE_AVAILABILITY_TIMES_FILENAME;
			File fout = new File(filename);
			FileOutputStream fos = new FileOutputStream(fout);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

			for (int idx = 0; idx < resourceCountDefaultAvailability; ++idx) {
				double availabilityTime = Constants.DEFAULT_RESOURCE_AVAILABILITY_TIME;
				bw.write(dft.format(availabilityTime));
				bw.newLine();
			}

			for (int idx = 0; idx < resourceCount; ++idx) {
				double availabilityTime = (int) availabilityTimes[idx];
				bw.write(dft.format(availabilityTime));
				bw.newLine();
			}

			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static double[] loadResourceAvailabilityTimes(int resourceCount) {
		double[] availabilityTimes = new double[resourceCount];

		try {
			String filename = Constants.RESOURCE_AVAILABILITY_TIMES_FILENAME;
			Scanner scanner = new Scanner(new File(filename));

			for (int idx = 0; idx < resourceCount; ++idx) {
				double availabilityTime = Double.parseDouble(scanner.nextLine());
				availabilityTimes[idx] = availabilityTime;
			}

			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		return availabilityTimes;
	}

	private static double getExponentialRandom(double rate) {
		// Generate exponentially distributed random numbers.
		return -Math.log(1.0 - Math.random()) / rate;
	}

	public static double getRandomNumber(double minNumber, double maxNumber) {
		// Generate a double number in the given range.
		Random random = new Random();
		double randomNumber = random.nextDouble() * (maxNumber - minNumber + 1) + minNumber;

		return randomNumber;
	}

	public static double getRandomRoundedNumber(double minNumber, double maxNumber) {
		// Generate a long number in the given range instead of a double number.
		long minNumberLong = Math.round(minNumber);
		long maxNumberLong = Math.round(maxNumber);

		Random random = new Random();
		long randomNumberLong = (long) (random.nextDouble() * (maxNumberLong - minNumberLong + 1)) + minNumberLong;

		return (double) randomNumberLong;
	}

}
