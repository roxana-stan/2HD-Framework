package dag_scheduling_algorithms;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.Scanner;

import javafx.util.Pair;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.lists.CloudletList;
import org.cloudbus.cloudsim.lists.VmList;
import org.edge.core.edge.EdgeDevice;

import scheduling_evaluation.DagUtils;
import scheduling_algorithms.DefaultEdgeCloudDatacenterBroker;
import scheduling_evaluation.Constants;
import scheduling_evaluation.SimulationUtils;
import scheduling_evaluation.Task;
import scheduling_evaluation.TaskGraph;
import scheduling_evaluation.Types.ResourceType;

public class DefaultDagEdgeCloudDatacenterBroker extends DefaultEdgeCloudDatacenterBroker {

	protected TaskGraph taskGraph;

	protected Map<Integer, Integer> taskToResourceMappings = null;

	protected LinkedList<Integer> sortedTasksByPriorityDesc = null;

	protected Map<Integer, Double> taskUpwardRankMappings = null;							// HEFT, RandHEFT.

	protected Map<Integer, Double> taskUtilityRankMappings = null;							// Utility, RandUtility.

	protected Map<Integer, Double> taskAFT = null;											// Tasks' actual finish times.

	protected Map<Integer, List<Pair<Double, Double>>> resourceAllocatedTimeSlots = null;	// Sorted lists of times when resources execute the assigned tasks.

	protected int resourceAllocatedTimeSlotIdx = Constants.INVALID_RESULT_INT;				// Temporary value used when allocating a task to a resource
																							// to insert a new time slot at a specific index in an ordered list of slots.

	/* Used only for dynamic task scheduling. */
	protected int taskSubgraphCount = Constants.INVALID_RESULT_INT;
	protected List<Double> taskSubgraphArrivalTimes = null;
	protected List<Integer> taskSubgraphFirstTasks = null;
	protected List<String> taskSubgraphFilenames = null;

	private static final Integer seed = new Random().nextInt();

	public DefaultDagEdgeCloudDatacenterBroker(String name, TaskGraph taskGraph) throws Exception {
		super(name);

		this.taskGraph = taskGraph;
	}

	public TaskGraph getTaskGraph() {
		return this.taskGraph;
	}

	public void setTaskGraph(TaskGraph taskGraph) {
		this.taskGraph = taskGraph;
	}

	protected void initializeDynamicTaskSubgraphsInfo() {
		this.taskSubgraphCount = DagUtils.getTaskSubgraphCount();

		this.taskSubgraphArrivalTimes = SimulationUtils.loadTaskSubgraphArrivalTimes(this.taskSubgraphCount);

		try {
			Scanner scanner = new Scanner(new File(DagUtils.getTaskSubgraphsFilename()));

			this.taskSubgraphFirstTasks = new ArrayList<Integer>(this.taskSubgraphCount);
			this.taskSubgraphFilenames = new ArrayList<String>(this.taskSubgraphCount);

			for (int subgraphIdx = 1; subgraphIdx <= this.taskSubgraphCount; ++subgraphIdx) {
				Integer taskSubgraphFirstTask = Integer.parseInt(scanner.nextLine());
				this.taskSubgraphFirstTasks.add(taskSubgraphFirstTask);

				String taskSubgraphFilename = scanner.nextLine();
				this.taskSubgraphFilenames.add(taskSubgraphFilename);
			}

			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/* Upward ranks. */
	protected void computeUpwardRanks() {
		if (this.taskUpwardRankMappings == null) {
			this.taskUpwardRankMappings = new HashMap<Integer, Double>(this.taskGraph.getTaskCount());
		}

		Queue<Integer> tasks = new LinkedList<Integer>();
		tasks.addAll(this.taskGraph.getExitTasks());

		while (!tasks.isEmpty()) {
			Integer task = tasks.remove();
			// Check if the task's upward rank has been already computed.
			if (this.taskUpwardRankMappings.containsKey(task)) {
				continue;
			}
			// Check if the task has been already scheduled.
			if (this.taskToResourceMappings.containsKey(task)) {
				continue;
			}
			// Compute the task's upward rank, if possible.
			if (!computeUpwardRank(task)) {
				continue;
			}

			List<Pair<Integer, Double>> predTasksInfo = this.taskGraph.getPredecessorTasksInfo(task);
			for (Pair<Integer, Double> predTaskInfo : predTasksInfo) {
				Integer predTask = predTaskInfo.getKey();
				tasks.add(predTask);
			}
		}
	}

	private boolean computeUpwardRank(Integer task) {
		Double maxSuccTaskUpwardRank = 0.0;
		List<Pair<Integer, Double>> succTasksInfo = this.taskGraph.getSuccessorTasksInfo(task);
		for (Pair<Integer, Double> succTaskInfo : succTasksInfo) {
			Integer succTask = succTaskInfo.getKey();
			if (!this.taskUpwardRankMappings.containsKey(succTask)) {
				Log.printLine("Task " + task + " > Upward rank of the successor task " + succTask + " not yet computed!");
				return false;
			}

			Pair<Integer, Integer> dependency = new Pair<Integer, Integer>(task, succTask);
			Double succTaskUpwardRank = this.taskGraph.getAverageCommunicationCost(dependency) + this.taskUpwardRankMappings.get(succTask);
			if (succTaskUpwardRank > maxSuccTaskUpwardRank) {
				maxSuccTaskUpwardRank = succTaskUpwardRank;
			}
		}

		Double eps = 1.0 / Math.pow(10, 6);
		Double upwardRank = this.taskGraph.getAverageComputationCost(task) + maxSuccTaskUpwardRank + eps;
		this.taskUpwardRankMappings.put(task, upwardRank);
		return true;
	}

	protected void sortTasksByUpwardRanks() {
		Map<Integer, Integer> randomTieBreakers = new HashMap<Integer, Integer>();
		this.taskUpwardRankMappings.keySet().forEach(taskId -> {
			Random random = new Random(seed + taskId);
			randomTieBreakers.put(taskId, random.nextInt());
		});

		this.sortedTasksByPriorityDesc = new LinkedList<Integer>();
		this.taskUpwardRankMappings.entrySet()
								.stream()
								.sorted(Map.Entry.<Integer, Double>comparingByValue(Comparator.reverseOrder())
//										.thenComparingInt(entry -> entry.getKey().hashCode()))
										.thenComparingInt(entry -> randomTieBreakers.get(entry.getKey())))
								.forEachOrdered(entry -> this.sortedTasksByPriorityDesc.add(entry.getKey()));
	}

	protected void clearUpwardRanks() {
		this.taskUpwardRankMappings.clear();
		this.taskUpwardRankMappings = null;
	}

	/* Utility ranks. */
	protected void computeUtilityRanks() {
		if (this.taskUtilityRankMappings == null) {
			this.taskUtilityRankMappings = new HashMap<Integer, Double>(this.taskGraph.getTaskCount());
		}

		Queue<Integer> tasks = new LinkedList<Integer>();
		tasks.addAll(this.taskGraph.getExitTasks());

		while (!tasks.isEmpty()) {
			Integer task = tasks.remove();
			// Check if the task's utility rank has been already computed.
			if (this.taskUtilityRankMappings.containsKey(task)) {
				continue;
			}
			// Check if the task has been already scheduled.
			if (this.taskToResourceMappings.containsKey(task)) {
				continue;
			}
			// Compute the task's utility rank, if possible.
			if (!computeUtilityRank(task)) {
				continue;
			}

			List<Pair<Integer, Double>> predTasksInfo = this.taskGraph.getPredecessorTasksInfo(task);
			for (Pair<Integer, Double> predTaskInfo : predTasksInfo) {
				Integer predTask = predTaskInfo.getKey();
				tasks.add(predTask);
			}
		}
	}

	private boolean computeUtilityRank(Integer task) {
		Double succTaskUtilityRankSum = 1.0 / Math.pow(10, 6);
		List<Pair<Integer, Double>> succTasksInfo = this.taskGraph.getSuccessorTasksInfo(task);
		for (Pair<Integer, Double> succTaskInfo : succTasksInfo) {
			Integer succTask = succTaskInfo.getKey();
			if (!this.taskUtilityRankMappings.containsKey(succTask)) {
				Log.printLine("Task " + task + " > Utility rank of the successor task " + succTask + " not yet computed!");
				return false;
			}

			Double succTaskUtilityRank = this.taskUtilityRankMappings.get(succTask);
			succTaskUtilityRankSum += succTaskUtilityRank;
		}

		Double utilityRank = this.taskGraph.computeTaskOutputData(task) / (1.0 + this.taskGraph.getAverageComputationCost(task)) + succTaskUtilityRankSum;
		this.taskUtilityRankMappings.put(task, utilityRank);
		return true;
	}

	protected void sortTasksByUtilityRanks() {
		Map<Integer, Integer> randomTieBreakers = new HashMap<Integer, Integer>();
		this.taskUtilityRankMappings.keySet().forEach(taskId -> {
			Random random = new Random(seed + taskId);
			randomTieBreakers.put(taskId, random.nextInt());
		});

		this.sortedTasksByPriorityDesc = new LinkedList<Integer>();
		this.taskUtilityRankMappings.entrySet()
								.stream()
								.sorted(Map.Entry.<Integer, Double>comparingByValue(Comparator.reverseOrder())
//										.thenComparingInt(entry -> entry.getKey().hashCode()))
										.thenComparingInt(entry -> randomTieBreakers.get(entry.getKey())))
								.forEachOrdered(entry -> this.sortedTasksByPriorityDesc.add(entry.getKey()));
	}

	protected void clearUtilityRanks() {
		this.taskUtilityRankMappings.clear();
		this.taskUtilityRankMappings = null;
	}

	protected Double computeEST(Integer task, Integer resource) {
		// Task's computation time on the given resource.
		Double computationTime = this.taskGraph.getComputationCost(task, resource);

		// Task's arrival time.
		Cloudlet cloudlet = CloudletList.getById(getCloudletList(), task);
		Double taskArrivalTime = ((Task) cloudlet).getArrivalTime();
		// Resource's availability time.
		Double resourceAvailabilityTime = this.taskGraph.getResourcesAvailability().get(resource);
		// Consider both task's arrival time and resource's availability time.
		Double taskResourceAvailabilityTime = Math.max(taskArrivalTime, resourceAvailabilityTime);

		/*
		 * Compute ready time of the task on the resource: time when all predecessor tasks were executed
		 * and all communication data from predecessor tasks were transferred to the resource.
		 */
		Double taskReadyTime = 0.0;
		List<Pair<Integer, Double>> predTasksInfo = this.taskGraph.getPredecessorTasksInfo(task);
		for (Pair<Integer, Double> predTaskInfo : predTasksInfo) {
			Integer predTask = predTaskInfo.getKey();
			Integer predResource = this.taskToResourceMappings.get(predTask);

			Pair<Integer, Integer> tasksDependency = new Pair<Integer, Integer>(predTask, task);
			Pair<Integer, Integer> resources = new Pair<Integer, Integer>(predResource, resource);
			Double communicationTime = this.taskGraph.getCommunicationCost(tasksDependency, resources);

			Double predTaskReadyTime = Math.max(this.taskAFT.get(predTask), taskResourceAvailabilityTime) + communicationTime;
			if (predTaskReadyTime > taskReadyTime) {
				taskReadyTime = predTaskReadyTime;
			}
		}

		/* Find the earliest available time of the resource for the execution of the task, including the idle time slots. */
		this.resourceAllocatedTimeSlotIdx = 0;

		List<Pair<Double, Double>> allocatedTimeSlots = this.resourceAllocatedTimeSlots.get(resource);
		// No tasks were previously allocated on the given resource.
		if (allocatedTimeSlots.isEmpty()) {
			return Math.max(resourceAvailabilityTime, taskReadyTime);
		}

		// Try to find an idle slot.
		int allocatedTimeSlotCount = allocatedTimeSlots.size();
		while (this.resourceAllocatedTimeSlotIdx < allocatedTimeSlotCount) {
			Double idleSlotStartTime = this.resourceAllocatedTimeSlotIdx == 0 ? resourceAvailabilityTime : allocatedTimeSlots.get(this.resourceAllocatedTimeSlotIdx-1).getValue();
			Double idleSlotFinishTime = allocatedTimeSlots.get(this.resourceAllocatedTimeSlotIdx).getKey();

			if (Math.max(idleSlotStartTime, taskReadyTime) + computationTime <= idleSlotFinishTime) {
				return Math.max(idleSlotStartTime, taskReadyTime);
			}

			++this.resourceAllocatedTimeSlotIdx;
		}

		// Could not find any feasible idle slot, thus select the time when the resource completed the last task's execution.
		Double lastAllocatedSlotFinishTime = allocatedTimeSlots.get(allocatedTimeSlotCount-1).getValue();
		return Math.max(lastAllocatedSlotFinishTime, taskReadyTime);
	}

	protected double getEdgeDeviceBatteryConsumption(Cloudlet cloudlet, Vm vm) {
		ResourceType resourceType = SimulationUtils.getResourceType(vm);
		if (resourceType == ResourceType.CLOUD_RESOURCE) {
			// Cloud resource, not battery powered.
			return 0.0;
		}

		Integer task = cloudlet.getCloudletId();
		if (!this.taskToResourceMappings.containsKey(task)) {
			Log.printLine("Task " + task + " > No allocated resource");
			return Constants.INVALID_RESULT_DOUBLE;
		}
		Integer resource = this.taskToResourceMappings.get(task);
		if (!Objects.equals(resource, vm.getId())) {
			Log.printLine("Task " + task + " > Resource mismatch " + resource + " != " + vm.getId());
			return Constants.INVALID_RESULT_DOUBLE;
		}

		double processingTime = this.taskGraph.getComputationCost(task, resource);

		double transferTime = 0.0;
		// Input data transfer time.
		List<Pair<Integer, Double>> predTasksInfo = this.taskGraph.getPredecessorTasksInfo(task);
		for (Pair<Integer, Double> predTaskInfo : predTasksInfo) {
			Integer predTask = predTaskInfo.getKey();
			if (!this.taskToResourceMappings.containsKey(predTask)) {
				Log.printLine("Task " + predTask + " > No allocated resource");
				return Constants.INVALID_RESULT_DOUBLE;
			}
			Integer predResource = this.taskToResourceMappings.get(predTask);

			Pair<Integer, Integer> tasksDependency = new Pair<Integer, Integer>(predTask, task);
			Pair<Integer, Integer> resources = new Pair<Integer, Integer>(predResource, resource);

			transferTime += this.taskGraph.getCommunicationCost(tasksDependency, resources);
		}
		// Output data transfer time.
		List<Pair<Integer, Double>> succTasksInfo = this.taskGraph.getSuccessorTasksInfo(task);
		for (Pair<Integer, Double> succTaskInfo : succTasksInfo) {
			Integer succTask = succTaskInfo.getKey();
			if (!this.taskToResourceMappings.containsKey(succTask)) {
				Log.printLine("Task " + succTask + " > No allocated resource");
				return Constants.INVALID_RESULT_DOUBLE;
			}
			Integer succResource = this.taskToResourceMappings.get(succTask);

			Pair<Integer, Integer> tasksDependency = new Pair<Integer, Integer>(task, succTask);
			Pair<Integer, Integer> resources = new Pair<Integer, Integer>(resource, succResource);

			transferTime += this.taskGraph.getCommunicationCost(tasksDependency, resources);
		}

		double consumption = processingTime * 1.0;
		if (resourceType == ResourceType.EDGE_RESOURCE_MOBILE_PHONE) {
			consumption += transferTime * Constants.SMARTPHONE_PM_BATTERY_DRAINAGE_RATE;
		} else if (resourceType == ResourceType.EDGE_RESOURCE_RASPBERRY_PI) {
			consumption += transferTime * Constants.RASPBERRY_PI_PM_BATTERY_DRAINAGE_RATE;
		}

		return consumption;
	}

	protected double getEstimatedEdgeDeviceBatteryConsumption(Integer task, Integer resource) {
		ResourceType resourceType = this.taskGraph.getResources().get(resource);
		if (resourceType == ResourceType.CLOUD_RESOURCE) {
			// Cloud resource, not battery powered.
			return 0.0;
		}

		double processingTime = this.taskGraph.getComputationCost(task, resource);

		double transferTime = 0.0;
		// Input data transfer time.
		List<Pair<Integer, Double>> predTasksInfo = this.taskGraph.getPredecessorTasksInfo(task);
		for (Pair<Integer, Double> predTaskInfo : predTasksInfo) {
			Integer predTask = predTaskInfo.getKey();
			Pair<Integer, Integer> tasksDependency = new Pair<Integer, Integer>(predTask, task);

			double estimatedCommunicationCost = Constants.INVALID_RESULT_DOUBLE;
			if (this.taskToResourceMappings.containsKey(predTask)) {
				Integer predResource = this.taskToResourceMappings.get(predTask);
				Pair<Integer, Integer> resources = new Pair<Integer, Integer>(predResource, resource);

				estimatedCommunicationCost = this.taskGraph.getCommunicationCost(tasksDependency, resources);
			} else {
				// No allocated resource, consider the worst case scenario (the slowest resource having the maximum communication cost).
				for (Integer predResource : this.taskGraph.getResources().keySet()) {
					Pair<Integer, Integer> resources = new Pair<Integer, Integer>(predResource, resource);

					estimatedCommunicationCost = Math.max(estimatedCommunicationCost, this.taskGraph.getCommunicationCost(tasksDependency, resources));
				}
			}
			transferTime += estimatedCommunicationCost;
		}
		// Output data transfer time.
		List<Pair<Integer, Double>> succTasksInfo = this.taskGraph.getSuccessorTasksInfo(task);
		for (Pair<Integer, Double> succTaskInfo : succTasksInfo) {
			Integer succTask = succTaskInfo.getKey();
			Pair<Integer, Integer> tasksDependency = new Pair<Integer, Integer>(task, succTask);

			double estimatedCommunicationCost = Constants.INVALID_RESULT_DOUBLE;
			if (this.taskToResourceMappings.containsKey(succTask)) {
				Integer succResource = this.taskToResourceMappings.get(succTask);
				Pair<Integer, Integer> resources = new Pair<Integer, Integer>(resource, succResource);

				estimatedCommunicationCost = this.taskGraph.getCommunicationCost(tasksDependency, resources);
			} else {
				// No allocated resource, consider the worst case scenario (the slowest resource having the maximum communication cost).
				for (Integer succResource : this.taskGraph.getResources().keySet()) {
					Pair<Integer, Integer> resources = new Pair<Integer, Integer>(resource, succResource);

					estimatedCommunicationCost = Math.max(estimatedCommunicationCost, this.taskGraph.getCommunicationCost(tasksDependency, resources));
				}
			}
			transferTime += estimatedCommunicationCost;
		}

		double consumption = processingTime * 1.0;
		if (resourceType == ResourceType.EDGE_RESOURCE_MOBILE_PHONE) {
			consumption += transferTime * Constants.SMARTPHONE_PM_BATTERY_DRAINAGE_RATE;
		} else if (resourceType == ResourceType.EDGE_RESOURCE_RASPBERRY_PI) {
			consumption += transferTime * Constants.RASPBERRY_PI_PM_BATTERY_DRAINAGE_RATE;
		}

		return consumption;
	}

	protected boolean canExecuteTaskOnResourceWithLimitedBatteryCapacity(Integer task, Integer resource) {
		Vm vm = VmList.getById(getVmsCreatedList(), resource);
		ResourceType resourceType = SimulationUtils.getResourceType(vm);

		// A cloud resource can always execute the task.
		if (resourceType == ResourceType.CLOUD_RESOURCE) {
			return true;
		}

		// Check if the edge device has enough battery to execute the task.
		int datacenterId = getVmsToDatacentersMap().get(vm.getId());
		Datacenter datacenter = (Datacenter) CloudSim.getEntity(datacenterId);

		List<Host> hostList = datacenter.getHostList();
		for (Host host : hostList) {
			if (!host.getVmList().contains(vm)) {
				continue;
			}

			EdgeDevice edgeDevice = (EdgeDevice) host;
			if (!edgeDevice.isEnabled()) {
				return false;
			}

			double batteryConsumption = 0.0;
			for (Map.Entry<Integer, Integer> taskResourceEntry : this.taskToResourceMappings.entrySet()) {
				Integer scheduledTask = taskResourceEntry.getKey();
				Integer allocatedResource = taskResourceEntry.getValue();
				if (!Objects.equals(allocatedResource, resource)) {
					continue;
				}
				batteryConsumption += getEstimatedEdgeDeviceBatteryConsumption(scheduledTask, resource);
			}
			batteryConsumption += getEstimatedEdgeDeviceBatteryConsumption(task, resource);

			double maxBatteryCapacity = edgeDevice.getMaxBatteryCapacity();
			double currentBatteryCapacity = maxBatteryCapacity - batteryConsumption;
			// Have at least 20% remaining battery after processing.
			boolean lowBattery = (currentBatteryCapacity < (0.20 * maxBatteryCapacity));
			return !lowBattery;
		}

		return false;
	}

}
