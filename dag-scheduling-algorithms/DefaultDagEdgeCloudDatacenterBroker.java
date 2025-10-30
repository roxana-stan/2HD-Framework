package dag_scheduling_algorithms;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.lists.CloudletList;
import org.cloudbus.cloudsim.lists.VmList;
import org.edge.core.edge.EdgeDevice;

import scheduling_evaluation.DagUtils;
import scheduling_algorithms.DefaultEdgeCloudDatacenterBroker;
import scheduling_evaluation.Constants;
import scheduling_evaluation.Pair;
import scheduling_evaluation.SimulationUtils;
import scheduling_evaluation.Task;
import scheduling_evaluation.TaskGraph;
import scheduling_evaluation.TaskUtils;
import scheduling_evaluation.Types.ResourceType;

public class DefaultDagEdgeCloudDatacenterBroker extends DefaultEdgeCloudDatacenterBroker {

	protected TaskGraph taskGraph;

	protected Map<Integer, Integer> taskToResourceMappings = null;

	protected Map<Integer, Double> taskUpwardRankMappings = null;							// HEFT, RandHEFT, CPOP.
	protected Map<Integer, Double> taskDownwardRankMappings = null;							// CPOP.

	protected Map<Integer, Double> taskHeftRankMappings = null;								// HEFT, RandHEFT.
	protected Map<Integer, Double> taskCpopRankMappings = null;								// CPOP.
	protected Map<Integer, Double> taskPetsRankMappings = null;								// PETS.

	protected Map<Integer, Double> taskUtilityRankMappings = null;							// Utility, RandUtility.

	protected Map<Integer, Double> taskAFT = null;											// Tasks' actual finish times.

	protected Map<Integer, List<Pair<Double, Double>>> resourceAllocatedTimeSlots = null;	// Sorted lists of times when resources execute the assigned tasks.

	protected int resourceAllocatedTimeSlotIdx = Constants.INVALID_RESULT_INT;				// Temporary value used when allocating a task to a resource
																							// to insert a new time slot at a specific index in an ordered list of slots.

	protected LinkedList<Integer> sortedTasksByPriorityDesc = null;

	protected PriorityQueue<Integer> cpopTasks = null;										// CPOP.
	protected Pair<List<Integer>, Integer> criticalPath = null;								// CPOP.

	private Map<Integer, LinkedList<Integer>> petsLevels = null;							// PETS.

	/* Used only for dynamic task scheduling. */
	protected int taskSubgraphCount = Constants.INVALID_RESULT_INT;
	protected List<Double> taskSubgraphArrivalTimes = null;
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

			this.taskSubgraphFilenames = new ArrayList<String>(this.taskSubgraphCount);
			for (int subgraphIdx = 1; subgraphIdx <= this.taskSubgraphCount; ++subgraphIdx) {
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

			Map<Integer, Double> predTasksInfo = this.taskGraph.getPredecessorTasksInfo(task);
			for (Integer predTask : predTasksInfo.keySet()) {
				tasks.add(predTask);
			}
		}
	}

	private boolean computeUpwardRank(Integer task) {
		Double maxSuccTaskUpwardRank = 0.0;
		Map<Integer, Double> succTasksInfo = this.taskGraph.getSuccessorTasksInfo(task);
		for (Integer succTask : succTasksInfo.keySet()) {
			if (!this.taskUpwardRankMappings.containsKey(succTask)) {
				Log.printLine("Task " + task + " > Upward rank of the successor task " + succTask + " not yet computed!");
				return false;
			}

			Double succTaskUpwardRank = this.taskGraph.getAverageCommunicationCost(task, succTask) + this.taskUpwardRankMappings.get(succTask);
			if (succTaskUpwardRank > maxSuccTaskUpwardRank) {
				maxSuccTaskUpwardRank = succTaskUpwardRank;
			}
		}

		Double eps = 1.0 / Math.pow(10, 6);
		Double upwardRank = this.taskGraph.getAverageComputationCost(task) + maxSuccTaskUpwardRank + eps;
		this.taskUpwardRankMappings.put(task, upwardRank);
		return true;
	}

	protected void clearUpwardRanks() {
		this.taskUpwardRankMappings.clear();
		this.taskUpwardRankMappings = null;
	}

	/* Downward ranks. */
	protected void computeDownwardRanks() {
		if (this.taskDownwardRankMappings == null) {
			this.taskDownwardRankMappings = new HashMap<Integer, Double>(this.taskGraph.getTaskCount());
		}

		Queue<Integer> tasks = new LinkedList<Integer>();
		tasks.addAll(this.taskGraph.getEntryTasks());

		while (!tasks.isEmpty()) {
			Integer task = tasks.remove();
			// Check if the task's downward rank has been already computed.
			if (this.taskDownwardRankMappings.containsKey(task)) {
				continue;
			}
			// Check if the task has been already scheduled.
			if (this.taskToResourceMappings.containsKey(task)) {
				continue;
			}
			// Compute the task's downward rank, if possible.
			if (!computeDownwardRank(task)) {
				continue;
			}

			Map<Integer, Double> succTasksInfo = this.taskGraph.getSuccessorTasksInfo(task);
			for (Integer succTask : succTasksInfo.keySet()) {
				tasks.add(succTask);
			}
		}
	}

	private boolean computeDownwardRank(Integer task) {
		Double maxPredTaskDownwardRank = 0.0;
		Map<Integer, Double> predTasksInfo = this.taskGraph.getPredecessorTasksInfo(task);
		for (Integer predTask : predTasksInfo.keySet()) {
			if (!this.taskDownwardRankMappings.containsKey(predTask)) {
				Log.printLine("Task " + task + " > Downward rank of the predecessor task " + predTask + " not yet computed!");
				return false;
			}

			Double predTaskDownwardRank = this.taskDownwardRankMappings.get(predTask)
					+ this.taskGraph.getAverageComputationCost(predTask)
					+ this.taskGraph.getAverageCommunicationCost(predTask, task);
			if (predTaskDownwardRank > maxPredTaskDownwardRank) {
				maxPredTaskDownwardRank = predTaskDownwardRank;
			}
		}

		Double eps = 1.0 / Math.pow(10, 6);
		Double downwardRank = maxPredTaskDownwardRank + eps;
		this.taskDownwardRankMappings.put(task, downwardRank);
		return true;
	}

	protected void clearDownwardRanks() {
		this.taskDownwardRankMappings.clear();
		this.taskDownwardRankMappings = null;
	}

	/* HEFT ranks. */
	protected void computeHeftRanks() {
		computeUpwardRanks();

		if (this.taskHeftRankMappings == null) {
			this.taskHeftRankMappings = new HashMap<Integer, Double>(this.taskGraph.getTaskCount());
		}

		for (Integer task : this.taskGraph.getTasks()) {
			if (!this.taskUpwardRankMappings.containsKey(task)) {
				Log.printLine("Task " + task + " > Upward rank not computed!");
				return;
			}
			Double heftRank = this.taskUpwardRankMappings.get(task);
			this.taskHeftRankMappings.put(task, heftRank);
		}
	}

	protected void sortTasksByHeftRanks() {
		Map<Integer, Integer> randomTieBreakers = new HashMap<Integer, Integer>();
		this.taskHeftRankMappings.keySet().forEach(taskId -> {
			Random random = new Random(seed + taskId);
			randomTieBreakers.put(taskId, random.nextInt());
		});

		this.sortedTasksByPriorityDesc = new LinkedList<Integer>();
		this.taskHeftRankMappings.entrySet()
								.stream()
								.sorted(Map.Entry.<Integer, Double>comparingByValue(Comparator.reverseOrder())
//										.thenComparingInt(entry -> entry.getKey().hashCode()))
										.thenComparingInt(entry -> randomTieBreakers.get(entry.getKey())))
								.forEachOrdered(entry -> this.sortedTasksByPriorityDesc.add(entry.getKey()));
	}

	protected void clearHeftRanks() {
		clearUpwardRanks();
	}

	/* CPOP ranks. */
	protected void computeCpopRanks() {
		computeUpwardRanks();
		computeDownwardRanks();

		if (this.taskCpopRankMappings == null) {
			this.taskCpopRankMappings = new HashMap<Integer, Double>(this.taskGraph.getTaskCount());
		}

		for (Integer task : this.taskGraph.getTasks()) {
			if (!this.taskUpwardRankMappings.containsKey(task)) {
				Log.printLine("Task " + task + " > Upward rank not computed!");
				return;
			}
			if (!this.taskDownwardRankMappings.containsKey(task)) {
				Log.printLine("Task " + task + " > Downward rank not computed!");
				return;
			}
			Double cpopRank = this.taskUpwardRankMappings.get(task) + this.taskDownwardRankMappings.get(task);
			this.taskCpopRankMappings.put(task, cpopRank);
		}
	}

	protected void clearCpopRanks() {
		clearUpwardRanks();
		clearDownwardRanks();
	}

	protected void findCriticalPath() {
		List<Integer> criticalPathTasks = new LinkedList<Integer>();
		Integer task = this.taskGraph.getEntryTasks().get(0);
		criticalPathTasks.add(task);
		while (!this.taskGraph.isExitTask(task)) {
			Double maxSuccTaskCpopRank = Constants.INVALID_RESULT_DOUBLE;
			Set<Integer> succTasks = this.taskGraph.getSuccessorTasksInfo(task).keySet();
			for (Integer succTask : succTasks) {
				Double succTaskCpopRank = this.taskCpopRankMappings.get(succTask);
				if (succTaskCpopRank > maxSuccTaskCpopRank) {
					maxSuccTaskCpopRank = succTaskCpopRank;
					task = succTask;
				}
			}
			criticalPathTasks.add(task);
		}

		Map<Integer, Double> criticalPathComputationTimes = new HashMap<Integer, Double>();
		for (Integer resource : this.taskGraph.getResources().keySet()) {
			criticalPathComputationTimes.put(resource, 0.0);
		}
		Set<Integer> ineligibleResources = new HashSet<Integer>();
		for (Integer criticalPathTask : criticalPathTasks) {
			double criticalPathTaskDataSize = this.taskGraph.getTaskInputData(criticalPathTask);
			for (Map.Entry<Integer, ResourceType> resourceEntry : this.taskGraph.getResources().entrySet()) {
				Integer resource = resourceEntry.getKey();
				ResourceType resourceType = resourceEntry.getValue();
				if (ineligibleResources.contains(resource)) {
					continue;
				}
				if (!TaskUtils.canExecuteTaskOnResourceWithLimitedMemoryCapacity(criticalPathTaskDataSize, resourceType)
					|| !canExecuteTaskOnResourceWithLimitedBatteryCapacity(criticalPathTask, resource)) {
					ineligibleResources.add(resource);
					criticalPathComputationTimes.put(resource, Double.MAX_VALUE);
					continue;
				}
				Double criticalPathComputationTime = criticalPathComputationTimes.get(resource) + this.taskGraph.getComputationCost(criticalPathTask, resource);
				criticalPathComputationTimes.put(resource, criticalPathComputationTime);
			}
		}
		Integer criticalPathResource = Constants.INVALID_RESULT_INT;
		Double minCriticalPathComputationTime = Double.MAX_VALUE;
		for (Integer resource : this.taskGraph.getResources().keySet()) {
			Double criticalPathComputationTime = criticalPathComputationTimes.get(resource);
			if (criticalPathComputationTime < minCriticalPathComputationTime) {
				minCriticalPathComputationTime = criticalPathComputationTime;
				criticalPathResource = resource;
			}
		}

		Log.printLine("CPOP critical path -- tasks: " + criticalPathTasks.toString() + " resource: " + criticalPathResource);
		this.criticalPath = new Pair<List<Integer>, Integer>(criticalPathTasks, criticalPathResource);
	}

	/* PETS ranks. */
	protected void computePetsRanks() {
		/* Phase 1: level sorting. */
		Map<Integer, Integer> taskIndegree = new HashMap<Integer, Integer>();
		Map<Integer, Integer> taskLevelMappings = new HashMap<Integer, Integer>();
		Queue<Integer> taskQueue = new LinkedList<Integer>();

		for (Integer task : this.taskGraph.getTasks()) {
			int predTaskCount = this.taskGraph.getPredecessorTasksInfo(task).size();
			taskIndegree.put(task, predTaskCount);
			if (predTaskCount == 0) {
				taskLevelMappings.put(task, 0);
				taskQueue.add(task);
			}
		}

		while (!taskQueue.isEmpty()) {
			Integer task = taskQueue.poll();
			int level = taskLevelMappings.get(task);
			Map<Integer, Double> succTasksInfo = this.taskGraph.getSuccessorTasksInfo(task);
			for (Integer succTask : succTasksInfo.keySet()) {
				taskIndegree.put(succTask, taskIndegree.get(succTask) - 1);
				if (taskIndegree.get(succTask) == 0) {
					taskLevelMappings.put(succTask, level + 1);
					taskQueue.add(succTask);
				}
			}
		}

		this.petsLevels = taskLevelMappings.entrySet()
				.stream()
				.collect(Collectors.groupingBy(
						Map.Entry::getValue,
						TreeMap::new,
						Collectors.mapping(Map.Entry::getKey, Collectors.toCollection(LinkedList::new))));

		/* Phase 2: task prioritization. */
		if (this.taskPetsRankMappings == null) {
			this.taskPetsRankMappings = new HashMap<Integer, Double>(this.taskGraph.getTaskCount());
		}

		for (Map.Entry<Integer, LinkedList<Integer>> levelEntry : this.petsLevels.entrySet()) {
			Integer level = levelEntry.getKey();
			LinkedList<Integer> tasks = levelEntry.getValue();
			Log.printLine("Level " + level + " > Tasks " + tasks);

			for (Integer task : tasks) {
				// Check if the task's PETS rank has been already computed.
				if (this.taskPetsRankMappings.containsKey(task)) {
					continue;
				}
				// Check if the task has been already scheduled.
				if (this.taskToResourceMappings.containsKey(task)) {
					continue;
				}
				// Compute the task's PETS rank, if possible.
				if (!computePetsRank(task)) {
					continue;
				}
			}

			tasks.sort((task1, task2) -> Double.compare(taskPetsRankMappings.get(task2), taskPetsRankMappings.get(task1)));
			Log.printLine("Level " + level + " > Tasks (sorted in descending order of PETS rank): " + tasks);
		}
	}

	private boolean computePetsRank(Integer task) {
		Double dataTransferCost = 0.0;
		Map<Integer, Double> succTasksInfo = this.taskGraph.getSuccessorTasksInfo(task);
		for (Integer succTask : succTasksInfo.keySet()) {
			dataTransferCost += this.taskGraph.getAverageCommunicationCost(task, succTask);
		}

		Double dataReceivingCost = 0.0;
		Map<Integer, Double> predTasksInfo = this.taskGraph.getPredecessorTasksInfo(task);
		for (Integer predTask : predTasksInfo.keySet()) {
			if (!this.taskPetsRankMappings.containsKey(predTask)) {
				Log.printLine("Task " + task + " > PETS rank of the predecessor task " + predTask + " not yet computed!");
				return false;
			}

			Double predTaskPetsRank = this.taskPetsRankMappings.get(predTask);
			if (predTaskPetsRank > dataReceivingCost) {
				dataReceivingCost = predTaskPetsRank;
			}
		}

		Double averageComputationCost = this.taskGraph.getAverageComputationCost(task);

		Double eps = 1.0 / Math.pow(10, 6);
		Double petsRank = dataTransferCost + dataReceivingCost + averageComputationCost + eps;
		this.taskPetsRankMappings.put(task, petsRank);
		return true;
	}

	protected void sortTasksByPetsRanks() {
		this.sortedTasksByPriorityDesc = new LinkedList<Integer>();
		for (LinkedList<Integer> tasks : this.petsLevels.values()) {
			for (Integer task : tasks) {
				// Check if the task has been already scheduled.
				if (!this.taskToResourceMappings.containsKey(task)) {
					this.sortedTasksByPriorityDesc.add(task);
				}
			}
		}
	}

	protected void clearPetsRanks() {
		this.taskPetsRankMappings.clear();
		this.taskPetsRankMappings = null;
	}

	/* Utility ranks. */
	protected void computeUtilityRanks(boolean hybrid) {
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
			if (!computeUtilityRank(task, hybrid)) {
				continue;
			}

			Map<Integer, Double> predTasksInfo = this.taskGraph.getPredecessorTasksInfo(task);
			for (Integer predTask : predTasksInfo.keySet()) {
				tasks.add(predTask);
			}
		}
	}

	private boolean computeUtilityRank(Integer task, boolean hybrid) {
		return computeUtilityRank(task);
	}

	private boolean computeUtilityRank(Integer task) {
		Double succTaskUtilityRankSum = 1.0 / Math.pow(10, 6);
		Map<Integer, Double> succTasksInfo = this.taskGraph.getSuccessorTasksInfo(task);
		for (Integer succTask : succTasksInfo.keySet()) {
			if (!this.taskUtilityRankMappings.containsKey(succTask)) {
				Log.printLine("Task " + task + " > Utility rank of the successor task " + succTask + " not yet computed!");
				return false;
			}

			Double succTaskUtilityRank = this.taskUtilityRankMappings.get(succTask);
			succTaskUtilityRankSum += succTaskUtilityRank;
		}

		Double utilityRank = this.taskGraph.getTaskOutputData(task) / (1.0 + this.taskGraph.getAverageComputationCost(task)) + succTaskUtilityRankSum;
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

	protected boolean isReadyTask(Integer task) {
		for (Integer predTask : this.taskGraph.getPredecessorTasksInfo(task).keySet()) {
			if (!this.taskToResourceMappings.containsKey(predTask)) {
				return false;
			}
		}
		return true;
	}

	protected Double getCloudletComputationTime(Integer task, Integer resource) {
		if (this.taskGraph.isEntryTask(task) || this.taskGraph.isExitTask(task)) {
			// Return non-zero computation time for the pseudo-entry / pseudo-exit cloudlets.
			return 1.0;
		}
		return this.taskGraph.getComputationCost(task, resource);
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
		Double taskReadyTime = taskArrivalTime;
		Map<Integer, Double> predTasksInfo = this.taskGraph.getPredecessorTasksInfo(task);
		for (Integer predTask : predTasksInfo.keySet()) {
			Integer predResource = this.taskToResourceMappings.get(predTask);

			Double communicationTime = this.taskGraph.getCommunicationCost(predTask, task, predResource, resource);
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
		Map<Integer, Double> predTasksInfo = this.taskGraph.getPredecessorTasksInfo(task);
		for (Integer predTask : predTasksInfo.keySet()) {
			if (!this.taskToResourceMappings.containsKey(predTask)) {
				Log.printLine("Task " + predTask + " > No allocated resource");
				return Constants.INVALID_RESULT_DOUBLE;
			}
			Integer predResource = this.taskToResourceMappings.get(predTask);

			transferTime += this.taskGraph.getCommunicationCost(predTask, task, predResource, resource);
		}
		// Output data transfer time.
		Map<Integer, Double> succTasksInfo = this.taskGraph.getSuccessorTasksInfo(task);
		for (Integer succTask : succTasksInfo.keySet()) {
			if (!this.taskToResourceMappings.containsKey(succTask)) {
				Log.printLine("Task " + succTask + " > No allocated resource");
				return Constants.INVALID_RESULT_DOUBLE;
			}
			Integer succResource = this.taskToResourceMappings.get(succTask);

			transferTime += this.taskGraph.getCommunicationCost(task, succTask, resource, succResource);
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
		Map<Integer, Double> predTasksInfo = this.taskGraph.getPredecessorTasksInfo(task);
		for (Integer predTask : predTasksInfo.keySet()) {
			double estimatedCommunicationCost = Constants.INVALID_RESULT_DOUBLE;
			if (this.taskToResourceMappings.containsKey(predTask)) {
				Integer predResource = this.taskToResourceMappings.get(predTask);
				estimatedCommunicationCost = this.taskGraph.getCommunicationCost(predTask, task, predResource, resource);
			} else {
				// No allocated resource, consider the worst case scenario (the slowest resource having the maximum communication cost).
				for (Integer predResource : this.taskGraph.getResources().keySet()) {
					estimatedCommunicationCost = Math.max(estimatedCommunicationCost, this.taskGraph.getCommunicationCost(predTask, task, predResource, resource));
				}
			}
			transferTime += estimatedCommunicationCost;
		}
		// Output data transfer time.
		Map<Integer, Double> succTasksInfo = this.taskGraph.getSuccessorTasksInfo(task);
		for (Integer succTask : succTasksInfo.keySet()) {
			double estimatedCommunicationCost = Constants.INVALID_RESULT_DOUBLE;
			if (this.taskToResourceMappings.containsKey(succTask)) {
				Integer succResource = this.taskToResourceMappings.get(succTask);
				estimatedCommunicationCost = this.taskGraph.getCommunicationCost(task, succTask, resource, succResource);
			} else {
				// No allocated resource, consider the worst case scenario (the slowest resource having the maximum communication cost).
				for (Integer succResource : this.taskGraph.getResources().keySet()) {
					estimatedCommunicationCost = Math.max(estimatedCommunicationCost, this.taskGraph.getCommunicationCost(task, succTask, resource, succResource));
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
		EdgeDevice edgeDevice = (EdgeDevice) vm.getHost();
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

}
