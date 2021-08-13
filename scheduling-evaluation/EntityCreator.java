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
	 * @param arrivalTimes Array of tasks' arrival times.
	 * @return The created task list.
	 */
	public static List<Task> createTasks(int userId, int taskId, double[] arrivalTimes) {
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
				Task task = createTask(userId, taskId++, taskType, arrivalTimes[taskIdx++]);
				taskList.add(task);
			}
		}

		// Sort the tasks based on their arrival times.
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

		return taskList;
	}

	/**
	 * Creates a task.
	 * @param userId User or broker ID.
	 * @param taskId ID of the task to be created.
	 * @param type Type of task to be created.
	 * @param arrivalTime Arrival time of the task.
	 * @return The created task.
	 */
	public static Task createTask(int userId, int taskId, TaskType type, double arrivalTime) {
		UtilizationModel utilizationModel = new UtilizationModelFull();

		Task task = new Task(taskId, TaskUtils.getTaskLength(type), Constants.TASK_PES_NUMBER,
							TaskUtils.getTaskFileSize(type), TaskUtils.getTaskOutputSize(type),
							utilizationModel, utilizationModel, utilizationModel);
		task.setUserId(userId);
		task.setType(type);
		task.setArrivalTime(arrivalTime);

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
