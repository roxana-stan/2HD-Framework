package scheduling_evaluation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

import scheduling_algorithms.DefaultCloudDatacenterBroker;
import scheduling_algorithms.DefaultEdgeCloudDatacenterBroker;
import scheduling_algorithms.MaxMinEdgeCloudDatacenterBroker;
import scheduling_algorithms.MinMinEdgeCloudDatacenterBroker;
import scheduling_algorithms.RoundRobinEdgeCloudDatacenterBroker;
import scheduling_algorithms.SjfEdgeCloudDatacenterBroker;
import scheduling_evaluation.Types.BrokerType;
import scheduling_evaluation.Types.DatacenterType;
import scheduling_evaluation.Types.TaskType;

public class EntityCreator {

	/**
	 * Creates a list of cloud resources.
	 * @param userId User or broker ID.
	 * @param count Number of VM cloud resources to be created.
	 * @param resId ID of the first resource to be created.
	 * @return The created cloud resource list.
	 */
	public static List<Vm> createCloudResources(int userId, int count, int resId) {
		LinkedList<Vm> vmList = new LinkedList<Vm>();

		for (int idx = 0; idx < count; idx++) {
			Vm vm = new Vm(resId++, userId,
						Constants.CLOUD_VM_MIPS, Constants.CLOUD_VM_PES_NUMBER,
						Constants.CLOUD_VM_RAM, Constants.CLOUD_VM_BANDWIDTH,
						Constants.CLOUD_VM_STORAGE, Constants.VMM, new CloudletSchedulerSpaceShared());

			vmList.add(vm);
		}

		return vmList;
	}

	/**
	 * Creates a list of tasks considering the distribution of task types.
	 * @param userId User or broker ID.
	 * @param taskId ID of the first task to be created.
	 * @param taskArrivalTimes Array of tasks' arrival times.
	 * @return The created task list.
	 */
	public static List<Task> createTasks(int userId, int taskId, double[] taskArrivalTimes) {
		LinkedList<Task> taskList = new LinkedList<Task>();

		// Compute the distribution of the tasks.
		Map<TaskType, Integer> taskDistribution = TaskUtils.getTaskDistribution();
		if (taskDistribution == null) {
			return null;
		}

		// Create the tasks.
		int taskIdx = 0;
		for (Map.Entry<TaskType, Integer> entry : taskDistribution.entrySet()) {
			TaskType taskType = entry.getKey();
			int taskCount = entry.getValue();

			for (int idx = 0; idx < taskCount; ++idx) {
				Task task = createTask(userId, taskId++, taskType, taskArrivalTimes[taskIdx++]);
				taskList.add(task);
			}
		}

		// Sort the tasks based on their arrival times.
		sortTasksByArrivalTime(taskList);

		return taskList;
	}

	/**
	 * Creates a list of tasks of generic type.
	 * @param userId User or broker ID.
	 * @param taskId ID of the first task to be created.
	 * @param taskCount Number of tasks to be created.
	 * @param taskLengths Array of tasks' lengths.
	 * @param taskFileSizes Array of tasks' input file sizes.
	 * @param taskOutputSizes Array of tasks' output file sizes.
	 * @param taskArrivalTimes Array of tasks' arrival times.
	 * @return The created task list.
	 */
	public static List<Task> createGenericTasks(int userId, int taskId, int taskCount, double[] taskLengths,
												double[] taskFileSizes, double[] taskOutputSizes, double[] taskArrivalTimes) {
		LinkedList<Task> taskList = new LinkedList<Task>();

		// Create the tasks.
		for (int taskIdx = 0; taskIdx < taskCount; ++taskIdx) {
			Task task = createGenericTask(userId, taskId++, taskLengths[taskIdx], taskFileSizes[taskIdx], taskOutputSizes[taskIdx], taskArrivalTimes[taskIdx]);
			taskList.add(task);
		}

		// Sort the tasks based on their arrival times.
		sortTasksByArrivalTime(taskList);

		return taskList;
	}

	/**
	 * Sorts the tasks in increasing order of their arrival times.
	 * @param taskList List of tasks to be sorted.
	 */
	private static void sortTasksByArrivalTime(LinkedList<Task> taskList) {
		taskList.sort(new Comparator<Task>() {
			@Override
			public int compare(Task task1, Task task2) {
				return Double.compare(task1.getArrivalTime(), task2.getArrivalTime());
			}
		});

		// Debugging: print the sorted list of cloudlets.
		for (Task task : taskList) {
			Log.printLine("Task " + task.getCloudletId() + ", type " + task.getType() +  " - Arrival time: " + task.getArrivalTime());
		}
	}

	/**
	 * Creates a task of the specified type.
	 * @param userId User or broker ID.
	 * @param taskId ID of the task to be created.
	 * @param taskType Type of the task to be created.
	 * @param taskArrivalTime Arrival time of the task.
	 * @return The created task.
	 */
	public static Task createTask(int userId, int taskId, TaskType taskType, double taskArrivalTime) {
		UtilizationModel utilizationModel = new UtilizationModelFull();

		Task task = new Task(taskId, TaskUtils.getTaskLength(taskType), Constants.TASK_PES_NUMBER,
							TaskUtils.getTaskFileSize(taskType), TaskUtils.getTaskOutputSize(taskType),
							utilizationModel, utilizationModel, utilizationModel);
		task.setUserId(userId);
		task.setType(taskType);
		task.setArrivalTime(taskArrivalTime);

		return task;
	}

	/**
	 * Creates a task of generic type.
	 * @param userId User or broker ID.
	 * @param taskId ID of the task to be created.
	 * @param taskLength Number of instructions of the task.
	 * @param taskFileSize File size of the task before execution.
	 * @param taskOutputSize File size of the task after execution.
	 * @param taskArrivalTime Arrival time of the task.
	 * @return The created task.
	 */
	public static Task createGenericTask(int userId, int taskId, double taskLength,
										double taskFileSize, double taskOutputSize, double taskArrivalTime) {
		UtilizationModel utilizationModel = new UtilizationModelFull();

		Task task = new Task(taskId, taskLength, Constants.TASK_PES_NUMBER,
							taskFileSize, taskOutputSize,
							utilizationModel, utilizationModel, utilizationModel);
		task.setUserId(userId);
		task.setType(TaskType.GENERIC);
		task.setArrivalTime(taskArrivalTime);

		return task;
	}

	/**
	 * Creates a DatacenterBroker of the indicated type.
	 * @param nameSuffix Broker name suffix, added to the broker type string constant.
	 * @param type Broker type, implementing a specific task scheduling algorithm.
	 * @return The created DatacenterBroker object.
	 */
	public static DatacenterBroker createBroker(String nameSuffix, BrokerType type) {
		DatacenterBroker broker = null;

		try {
			String brokerName = Types.brokerTypeString(type) + nameSuffix;

			switch (type) {
			case CLOUD_ONLY_DEFAULT_BROKER: {
				broker = new DefaultCloudDatacenterBroker(brokerName);
				break;
			}
			case EDGE_CLOUD_DEFAULT_BROKER: {
				broker = new DefaultEdgeCloudDatacenterBroker(brokerName);
				break;
			}
			case EDGE_CLOUD_ROUND_ROBIN_BROKER: {
				broker = new RoundRobinEdgeCloudDatacenterBroker(brokerName);
				break;
			}
			case EDGE_CLOUD_SJF_BROKER: {
				broker = new SjfEdgeCloudDatacenterBroker(brokerName);
				break;
			}
			case EDGE_CLOUD_MIN_MIN_BROKER: {
				broker = new MinMinEdgeCloudDatacenterBroker(brokerName);
				break;
			}
			case EDGE_CLOUD_MAX_MIN_BROKER: {
				broker = new MaxMinEdgeCloudDatacenterBroker(brokerName);
				break;
			}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

		return broker;
	}

	/**
	 * Creates a datacenter of cloud type.
	 * @param name Datacenter name.
	 * @param count Number of hosts to be created in datacenter.
	 * @return The created Datacenter object.
	 */
	public static Datacenter createCloudDatacenter(String name, int count) {
		/* Create a list to store physical machines. */
		List<Host> pmList = new ArrayList<Host>();

		for (int idx = 0; idx < count; ++idx) {
			// Create Pe list. Each Host has 1 Pe.
			List<Pe> peList = new ArrayList<Pe>();
			peList.add(new Pe(0, new PeProvisionerSimple(Constants.CLOUD_PM_MIPS)));

			// Create Host.
			Host host = new Host(idx,
								new RamProvisionerSimple(Constants.CLOUD_PM_RAM),
								new BwProvisionerSimple(Constants.CLOUD_PM_BANDWIDTH),
								Constants.CLOUD_PM_STORAGE,
								peList, new VmSchedulerTimeShared(peList));

			pmList.add(host);
		}

		/* Create DatacenterCharacteristics storing properties of a Datacenter. */
		double timeZone = 10.0;
		double costPerSec = 3.0;
		double costPerMem = 0.05;
		double costPerStorage = 0.1;
		double costPerBw = 0.1;
		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(Constants.ARCHITECTURE, Constants.OS, Constants.VMM,
																				pmList, timeZone,
																				costPerSec, costPerMem, costPerStorage, costPerBw);

		/* Create 1 Datacenter. */
		Datacenter datacenter = null;
		try {
			LinkedList<Storage> storageList = new LinkedList<Storage>();
			datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(pmList), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return datacenter;
	}

	/**
	 * Generates a datacenter name.
	 * @param nameSuffix Datacenter name suffix, added to the datacenter type string constant.
	 * @param type Datacenter type.
	 * @return The datacenter name.
	 */
	public static String getDatacenterName(String nameSuffix, DatacenterType type) {
		String datacenterName = Types.datacenterTypeString(type) + nameSuffix;
		return datacenterName;
	}

}
