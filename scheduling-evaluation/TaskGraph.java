package scheduling_evaluation;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.cloudbus.cloudsim.Log;

import scheduling_evaluation.Types.ResourceType;

public class TaskGraph {

	private List<Integer> entryTasks = null;
	private List<Integer> exitTasks = null;

	private Map<Integer, Map<Integer, Double>> graph = null;
	private Map<Integer, Map<Integer, Double>> reverseGraph = null;

	private Map<Integer, ResourceType> resources = null;
	private Map<Integer, Double> resourcesAvailability = null;

	private Map<Integer, Map<ResourceType, Double>> computationCosts = null;

	private Map<Integer, Double> taskInputData = null;
	private Map<Integer, Double> taskOutputData = null;
	private Map<Integer, Double> averageComputationCosts = null;
	private Map<Integer, Map<Integer, Double>> averageCommunicationCosts = null;

	private LinkedList<Integer> qlHeftSchedule = null;
	private LinkedList<Integer> ql2hdSchedule = null;

	public TaskGraph() {
		this.taskInputData = new HashMap<Integer, Double>();
		this.taskOutputData = new HashMap<Integer, Double>();
		this.averageComputationCosts = new HashMap<Integer, Double>();
		this.averageCommunicationCosts = new HashMap<Integer, Map<Integer, Double>>();
	}

	public void clearAndPrecomputeCosts() {
		// Clear costs.
		this.taskInputData.clear();
		this.taskOutputData.clear();
		this.averageComputationCosts.clear();
		this.averageCommunicationCosts.clear();

		// Precompute costs.
		for (Integer task : this.graph.keySet()) {
			// Input and output data.
			this.taskInputData.put(task, computeTaskInputData(task));
			this.taskOutputData.put(task, computeTaskOutputData(task));
		}
		for (Integer task : this.graph.keySet()) {
			// Computation and communication costs.
			this.averageComputationCosts.put(task, computeAverageComputationCost(task));
			this.averageCommunicationCosts.put(task, new HashMap<Integer, Double>());
			Map<Integer, Double> succTasksInfo = getSuccessorTasksInfo(task);
			for (Integer succTask : succTasksInfo.keySet()) {
				this.averageCommunicationCosts.get(task).put(succTask, computeAverageCommunicationCost(task, succTask));
			}
		}
	}

	public List<Integer> getEntryTasks() {
		return this.entryTasks;
	}

	public List<Integer> getExitTasks() {
		return this.exitTasks;
	}

	public Set<Integer> getTasks() {
		return this.graph.keySet();
	}

	private boolean notInitializedResources() {
		if (this.resources == null || this.resources.isEmpty()) {
			return true;
		}
		if (this.resourcesAvailability == null || this.resourcesAvailability.isEmpty()) {
			return true;
		}
		return false;
	}

	public void initializeResources(Map<Integer, ResourceType> resources, Map<Integer, Double> resourcesAvailability) {
		setResources(resources);
		setResourcesAvailability(resourcesAvailability);
	}

	public Map<Integer, ResourceType> getResources() {
		return this.resources;
	}

	private void setResources(Map<Integer, ResourceType> resources) {
		this.resources = resources;
	}

	public Map<Integer, Double> getResourcesAvailability() {
		return this.resourcesAvailability;
	}

	private void setResourcesAvailability(Map<Integer, Double> resourcesAvailability) {
		this.resourcesAvailability = resourcesAvailability;
	}

	public Map<Integer, Map<ResourceType, Double>> getComputationCosts() {
		return this.computationCosts;
	}

	public void addEntryTasks(List<Integer> entryTasks) {
		if (this.entryTasks == null) {
			this.entryTasks = new LinkedList<Integer>();
		}

		for (Integer entryTask : entryTasks) {
			addEntryTask(entryTask);
		}
	}

	public void addEntryTask(Integer entryTask) {
		if (this.entryTasks == null) {
			this.entryTasks = new LinkedList<Integer>();
		}

		this.entryTasks.add(entryTask);
	}

	public void addExitTasks(List<Integer> exitTasks) {
		if (this.exitTasks == null) {
			this.exitTasks = new LinkedList<Integer>();
		}

		for (Integer exitTask : exitTasks) {
			addExitTask(exitTask);
		}
	}

	public void addExitTask(Integer exitTask) {
		if (this.exitTasks == null) {
			this.exitTasks = new LinkedList<Integer>();
		}

		this.exitTasks.add(exitTask);
	}

	public void addTasks(List<Integer> tasks) {
		if (this.graph == null) {
			int taskCount = tasks.size();
			this.graph = new HashMap<Integer, Map<Integer, Double>>(taskCount);
			this.reverseGraph = new HashMap<Integer, Map<Integer, Double>>(taskCount);
		}

		for (Integer task : tasks) {
			addTask(task);
		}
	}

	public void addTask(Integer task) {
		if (this.graph == null) {
			this.graph = new HashMap<Integer, Map<Integer, Double>>();
			this.reverseGraph = new HashMap<Integer, Map<Integer, Double>>();
		}

		if (this.graph.containsKey(task)) {
			Log.printLine("Graph already contains task " + task);
			return;
		}

		this.graph.put(task, new HashMap<Integer, Double>());
		this.reverseGraph.put(task, new HashMap<Integer, Double>());
	}

	public void addDependencies(Map<Pair<Integer, Integer>, Double> dependencies) {
		for (Map.Entry<Pair<Integer, Integer>, Double> dependencyEntry : dependencies.entrySet()) {
			Pair<Integer, Integer> dependentTasks = dependencyEntry.getKey();
			Double dataDependency = dependencyEntry.getValue();

			Integer fromTask = dependentTasks.getKey();
			Integer toTask = dependentTasks.getValue();

			addDependency(fromTask, toTask, dataDependency);
		}
	}

	public void addDependency(Integer fromTask, Integer toTask, Double dataDependency) {
		this.graph.get(fromTask).put(toTask, dataDependency);
		this.reverseGraph.get(toTask).put(fromTask, dataDependency);
	}

	public void addComputationCosts(Map<Integer, Map<ResourceType, Double>> computationCosts) {
		if (this.computationCosts == null) {
			this.computationCosts = new HashMap<Integer, Map<ResourceType, Double>>(computationCosts.size());
		}

		for (Map.Entry<Integer, Map<ResourceType, Double>> taskComputationCostsEntry : computationCosts.entrySet()) {
			Integer task = taskComputationCostsEntry.getKey();
			Map<ResourceType, Double> resourceComputationCosts = taskComputationCostsEntry.getValue();

			addTaskComputationCosts(task, resourceComputationCosts);
		}
	}

	public void addTaskComputationCosts(Integer task, Map<ResourceType, Double> resourceComputationCosts) {
		if (notInitializedResources()) {
			System.out.println("Resources must be initialized beforehand");
			return;
		}

		if (this.computationCosts == null) {
			this.computationCosts = new HashMap<Integer, Map<ResourceType, Double>>();
		}

		if (this.computationCosts.containsKey(task)) {
			Log.printLine("Computation costs for task " + task + " already set");
			return;
		}

		// Add the task computation costs.
		this.computationCosts.put(task, resourceComputationCosts);
	}

	public int getTaskCount() {
		return this.graph.size();
	}

	public int getResourceCount() {
		return this.resources.size();
	}

	public Map<Integer, Double> getSuccessorTasksInfo(Integer task) {
		return this.graph.get(task);
	}

	public Map<Integer, Double> getPredecessorTasksInfo(Integer task) {
		return this.reverseGraph.get(task);
	}

	public double getDataDependency(Integer fromTask, Integer toTask) {
		Map<Integer, Double> succTasksInfo = getSuccessorTasksInfo(fromTask);
		return succTasksInfo.getOrDefault(toTask, Constants.INVALID_RESULT_DOUBLE);
	}

	public void printTaskGraph() {
		DecimalFormat dft = new DecimalFormat("###.##");

		for (Integer task : getTasks()) {
			String taskInfo = "";
			if (isEntryTask(task)) {
				taskInfo += " (entry)";
			}
			if (isExitTask(task)) {
				taskInfo += " (exit)";
			}

			Map<ResourceType, Double> resourceComputationCosts = this.computationCosts.get(task);
			Log.printLine("Task " + task + taskInfo
							+ " --- " + dft.format(getAverageComputationCost(task)) + " s (avg computation) " + resourceComputationCosts.toString());

			Log.printLine("  - Input: " + dft.format(getTaskInputData(task)) + " MB (data)");
			Map<Integer, Double> predTasksInfo = getPredecessorTasksInfo(task);
			for (Map.Entry<Integer, Double> predTaskInfo : predTasksInfo.entrySet()) {
				Integer predTask = predTaskInfo.getKey();
				Double dataDependency = predTaskInfo.getValue();
				Log.printLine("    * Dependency: Task " + predTask + " -> Task " + task
								+ " --- " + dft.format(dataDependency) + " MB (data) | " + dft.format(getAverageCommunicationCost(predTask, task)) + " s (avg communication)");
			}

			Log.printLine("  - Output: " + dft.format(getTaskOutputData(task)) + " MB (data)");
			Map<Integer, Double> succTasksInfo = getSuccessorTasksInfo(task);
			for (Map.Entry<Integer, Double> succTaskInfo : succTasksInfo.entrySet()) {
				Integer succTask = succTaskInfo.getKey();
				Double dataDependency = succTaskInfo.getValue();
				Log.printLine("    * Dependency: Task " + task + " -> Task " + succTask
								+ " --- " + dft.format(dataDependency) + " MB (data) | " + dft.format(getAverageCommunicationCost(task, succTask)) + " s (avg communication)");
			}

			Log.printLine();
		}
	}

	public boolean isEntryTask(Integer task) {
		return this.entryTasks.contains(task);
	}

	public boolean isExitTask(Integer task) {
		return this.exitTasks.contains(task);
	}

	public void removeExitTask(Integer task) {
		this.exitTasks.remove(task);
	}

	public Pair<Double, Double> computeDataDependencyLimits() {
		Double minDataDependency = Double.MAX_VALUE;
		Double maxDataDependency = Double.MIN_VALUE;

		for (Map<Integer, Double> dependentTasksInfo : this.graph.values()) {
			for (Double dataDependency : dependentTasksInfo.values()) {
				minDataDependency = Math.min(minDataDependency, dataDependency);
				maxDataDependency = Math.max(maxDataDependency, dataDependency);
			}
		}

		return new Pair<Double, Double>(minDataDependency, maxDataDependency);
	}

	public Double getTaskInputData(Integer task) {
		if (!this.taskInputData.containsKey(task)) {
			this.taskInputData.put(task, computeTaskInputData(task));
		}
		return this.taskInputData.get(task);
	}

	private Double computeTaskInputData(Integer task) {
		Double inputData = 0.0;

		Map<Integer, Double> predTasksInfo = getPredecessorTasksInfo(task);
		for (Double dataDependency : predTasksInfo.values()) {
			inputData += dataDependency;
		}

		return inputData;
	}

	public Double getTaskOutputData(Integer task) {
		if (!this.taskOutputData.containsKey(task)) {
			this.taskOutputData.put(task, computeTaskOutputData(task));
		}
		return this.taskOutputData.get(task);
	}

	private Double computeTaskOutputData(Integer task) {
		Double outputData = 0.0;

		Map<Integer, Double> succTasksInfo = getSuccessorTasksInfo(task);
		for (Double dataDependency : succTasksInfo.values()) {
			outputData += dataDependency;
		}

		return outputData;
	}

	public Double getAverageComputationCost(Integer task) {
		if (!this.averageComputationCosts.containsKey(task)) {
			this.averageComputationCosts.put(task, computeAverageComputationCost(task));
		}
		return this.averageComputationCosts.get(task);
	}

	private Double computeAverageComputationCost(Integer task) {
		Double computationCostsSum = 0.0;
		int costCount = 0;

		for (Map.Entry<Integer, ResourceType> resourceEntry : this.resources.entrySet()) {
			Integer resource = resourceEntry.getKey();
			ResourceType resourceType = resourceEntry.getValue();
			Double resourceAvailabilityTime = this.resourcesAvailability.get(resource);

			if (resourceAvailabilityTime > Constants.DEFAULT_RESOURCE_AVAILABILITY_TIME) {
				// Resource not available yet.
				continue;
			}
			if (!TaskUtils.canExecuteTaskOnResourceWithLimitedMemoryCapacity(getTaskInputData(task), resourceType)) {
				// Resource with limited memory capacity.
				continue;
			}

			computationCostsSum += getComputationCost(task, resource);
			++costCount;
		}

		return computationCostsSum / costCount;
	}

	public Double getComputationCost(Integer task, Integer resource) {
		ResourceType resourceType = this.resources.get(resource);

		return this.computationCosts.get(task).get(resourceType);
	}

	public Double getAverageCommunicationCost(Integer fromTask, Integer toTask) {
		if (!this.averageCommunicationCosts.containsKey(fromTask)) {
			this.averageCommunicationCosts.put(fromTask, new HashMap<Integer, Double>());
		}
		if (!this.averageCommunicationCosts.get(fromTask).containsKey(toTask)) {
			this.averageCommunicationCosts.get(fromTask).put(toTask, computeAverageCommunicationCost(fromTask, toTask));
		}
		return this.averageCommunicationCosts.get(fromTask).get(toTask);
	}

	private Double computeAverageCommunicationCost(Integer fromTask, Integer toTask) {
		Double communicationCostsSum = 0.0;
		int costCount = 0;

		for (Map.Entry<Integer, ResourceType> resourceEntry1 : this.resources.entrySet()) {
			Integer resource1 = resourceEntry1.getKey();
			ResourceType resourceType1 = resourceEntry1.getValue();
			Double resourceAvailabilityTime1 = this.resourcesAvailability.get(resource1);

			if (resourceAvailabilityTime1 > Constants.DEFAULT_RESOURCE_AVAILABILITY_TIME) {
				// Resource not available yet.
				continue;
			}
			if (!TaskUtils.canExecuteTaskOnResourceWithLimitedMemoryCapacity(getTaskInputData(fromTask), resourceType1)) {
				// Resource with limited memory capacity.
				continue;
			}

			for (Map.Entry<Integer, ResourceType> resourceEntry2 : this.resources.entrySet()) {
				Integer resource2 = resourceEntry2.getKey();
				ResourceType resourceType2 = resourceEntry2.getValue();
				Double resourceAvailabilityTime2 = this.resourcesAvailability.get(resource2);

				if (resourceAvailabilityTime2 > Constants.DEFAULT_RESOURCE_AVAILABILITY_TIME) {
					// Resource not available yet.
					continue;
				}
				if (!TaskUtils.canExecuteTaskOnResourceWithLimitedMemoryCapacity(getTaskInputData(toTask), resourceType2)) {
					// Resource with limited memory capacity.
					continue;
				}

				communicationCostsSum += getCommunicationCost(fromTask, toTask, resource1, resource2);
				++costCount;
			}
		}

		return communicationCostsSum / costCount;
	}

	public Double getCommunicationCost(Integer fromTask, Integer toTask, Integer fromResource, Integer toResource) {
		ResourceType fromResourceType = this.resources.get(fromResource);
		ResourceType toResourceType = this.resources.get(toResource);
		boolean fromEdgeResource = ResourceUtils.isEdgeResource(fromResourceType);
		boolean fromCloudResource = ResourceUtils.isCloudResource(fromResourceType);
		boolean toEdgeResource = ResourceUtils.isEdgeResource(toResourceType);
		boolean toCloudResource = ResourceUtils.isCloudResource(toResourceType);

		Double dataDependency = getDataDependency(fromTask, toTask);

		// Pseudo entry / exit tasks.
		if (isEntryTask(fromTask)) {
			// Cloud -> edge / cloud transfer.
			Double transferRate = toEdgeResource ? Constants.CLOUD_TO_EDGE_TRANSFER_RATE : Constants.CLOUD_TO_CLOUD_TRANSFER_RATE;
			return dataDependency / transferRate;
		}
		if (isExitTask(toTask)) {
			// Edge / cloud -> cloud transfer.
			Double transferRate = fromEdgeResource ? Constants.EDGE_TO_CLOUD_TRANSFER_RATE : Constants.CLOUD_TO_CLOUD_TRANSFER_RATE;
			return dataDependency / transferRate;
		}

		// Intraprocessor communication cost is negligible.
		if (Objects.equals(fromResource, toResource)) {
			return 0.0;
		}

		// Interprocessor communication cost.
		Double resourcesTransferRate = Constants.INVALID_RESULT_DOUBLE;
		if (fromEdgeResource && toEdgeResource) {
			// Edge -> edge transfer.
			resourcesTransferRate = Constants.EDGE_TO_EDGE_TRANSFER_RATE;
		} else if (fromEdgeResource && toCloudResource) {
			// Edge -> cloud transfer.
			resourcesTransferRate = Constants.EDGE_TO_CLOUD_TRANSFER_RATE;
		} else if (fromCloudResource && toEdgeResource) {
			// Cloud -> edge transfer.
			resourcesTransferRate = Constants.CLOUD_TO_EDGE_TRANSFER_RATE;
		} else if (fromCloudResource && toCloudResource) {
			// Cloud -> cloud transfer.
			resourcesTransferRate = Constants.CLOUD_TO_CLOUD_TRANSFER_RATE;
		}
		return dataDependency / resourcesTransferRate;
	}

	public LinkedList<Integer> getQlHeftSchedule() {
		return this.qlHeftSchedule;
	}

	public void setQlHeftSchedule(LinkedList<Integer> schedule) {
		this.qlHeftSchedule = schedule;
	}

	public LinkedList<Integer> getQl2hdSchedule() {
		return this.ql2hdSchedule;
	}

	public void setQl2hdSchedule(LinkedList<Integer> schedule) {
		this.ql2hdSchedule = schedule;
	}

}
