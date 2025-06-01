package scheduling_evaluation;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javafx.util.Pair;

import org.cloudbus.cloudsim.Log;

import scheduling_evaluation.Types.ResourceType;

public class TaskGraph {

	private List<Integer> entryTasks = null;
	private List<Integer> exitTasks = null;

	private Map<Integer, List<Pair<Integer, Double>>> graph = null;
	private Map<Integer, List<Pair<Integer, Double>>> reverseGraph = null;

	private Map<Integer, ResourceType> resources = null;
	private Map<Integer, Double> resourcesAvailability = null;

	private Map<Integer, Map<ResourceType, Double>> computationCosts = null;
	private Map<Integer, Double> averageComputationCosts = null;

	public TaskGraph() {
		this.averageComputationCosts = new HashMap<Integer, Double>();
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

			this.graph = new HashMap<Integer, List<Pair<Integer, Double>>>(taskCount);
			this.reverseGraph = new HashMap<Integer, List<Pair<Integer, Double>>>(taskCount);
		}

		for (Integer task : tasks) {
			addTask(task);
		}
	}

	public void addTask(Integer task) {
		if (this.graph == null) {
			this.graph = new HashMap<Integer, List<Pair<Integer, Double>>>();
			this.reverseGraph = new HashMap<Integer, List<Pair<Integer, Double>>>();
		}

		if (this.graph.containsKey(task)) {
			Log.printLine("Graph already contains task " + task);
			return;
		}

		this.graph.put(task, new LinkedList<Pair<Integer, Double>>());
		this.reverseGraph.put(task, new LinkedList<Pair<Integer, Double>>());
	}

	public void addDependencies(Map<Pair<Integer, Integer>, Double> dependencies) {
		for (Map.Entry<Pair<Integer, Integer>, Double> dependencyEntry : dependencies.entrySet()) {
			Pair<Integer, Integer> dependentTasks = dependencyEntry.getKey();
			Double dataDependency = dependencyEntry.getValue();

			addDependency(dependentTasks, dataDependency);
		}
	}

	public void addDependency(Pair<Integer, Integer> dependentTasks, Double dataDependency) {
		Integer fromTask = dependentTasks.getKey();
		Integer toTask = dependentTasks.getValue();

		this.graph.get(fromTask).add(new Pair<Integer, Double>(toTask, dataDependency));
		this.reverseGraph.get(toTask).add(new Pair<Integer, Double>(fromTask, dataDependency));
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
		// Also add the average task computation cost.
		this.averageComputationCosts.put(task, computeAverageComputationCost(task));
	}

	public int getTaskCount() {
		return this.graph.size();
	}

	public int getResourceCount() {
		return this.resources.size();
	}

	public List<Pair<Integer, Double>> getSuccessorTasksInfo(Integer task) {
		return this.graph.get(task);
	}

	public List<Pair<Integer, Double>> getPredecessorTasksInfo(Integer task) {
		return this.reverseGraph.get(task);
	}

	public double getDataDependency(Pair<Integer, Integer> dependency) {
		Integer fromTask = dependency.getKey();
		Integer toTask = dependency.getValue();

		List<Pair<Integer, Double>> succTasksInfo = getSuccessorTasksInfo(fromTask);
		for (Pair<Integer, Double> succTaskInfo : succTasksInfo) {
			Integer succTask = succTaskInfo.getKey();
			if (Objects.equals(succTask, toTask)) {
				Double dataDependency = succTaskInfo.getValue();
				return dataDependency;
			}
		}
		return Constants.INVALID_RESULT_DOUBLE;
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

			Log.printLine("  - Input: " + dft.format(computeTaskInputData(task)) + " MB (data)");
			List<Pair<Integer, Double>> predTasksInfo = getPredecessorTasksInfo(task);
			for (Pair<Integer, Double> predTaskInfo : predTasksInfo) {
				Integer predTask = predTaskInfo.getKey();
				Double dataDependency = predTaskInfo.getValue();
				Pair<Integer, Integer> tasksDependency = new Pair<Integer, Integer>(predTask, task);
				Log.printLine("    * Dependency: Task " + predTask + " -> Task " + task
								+ " --- " + dft.format(dataDependency) + " MB (data) | " + dft.format(getAverageCommunicationCost(tasksDependency)) + " s (avg communication)");
			}

			Log.printLine("  - Output: " + dft.format(computeTaskOutputData(task)) + " MB (data)");
			List<Pair<Integer, Double>> succTasksInfo = getSuccessorTasksInfo(task);
			for (Pair<Integer, Double> succTaskInfo : succTasksInfo) {
				Integer succTask = succTaskInfo.getKey();
				Double dataDependency = succTaskInfo.getValue();
				Pair<Integer, Integer> tasksDependency = new Pair<Integer, Integer>(task, succTask);
				Log.printLine("    * Dependency: Task " + task + " -> Task " + succTask
								+ " --- " + dft.format(dataDependency) + " MB (data) | " + dft.format(getAverageCommunicationCost(tasksDependency)) + " s (avg communication)");
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

		for (List<Pair<Integer, Double>> dependentTasksInfo : this.graph.values()) {
			for (Pair<Integer, Double> dependentTaskInfo : dependentTasksInfo) {
				Double dataDependency = dependentTaskInfo.getValue();

				minDataDependency = Math.min(minDataDependency, dataDependency);
				maxDataDependency = Math.max(maxDataDependency, dataDependency);
			}
		}

		return new Pair<Double, Double>(minDataDependency, maxDataDependency);
	}

	public Double computeTaskInputData(Integer task) {
		Double inputData = 0.0;

		List<Pair<Integer, Double>> predTasksInfo = getPredecessorTasksInfo(task);
		for (Pair<Integer, Double> predTaskInfo : predTasksInfo) {
			Double dataDependency = predTaskInfo.getValue();

			inputData += dataDependency;
		}

		return inputData;
	}

	public Double computeTaskOutputData(Integer task) {
		Double outputData = 0.0;

		List<Pair<Integer, Double>> succTasksInfo = getSuccessorTasksInfo(task);
		for (Pair<Integer, Double> succTaskInfo : succTasksInfo) {
			Double dataDependency = succTaskInfo.getValue();

			outputData += dataDependency;
		}

		return outputData;
	}

	public Double getAverageComputationCost(Integer task) {
		if (this.averageComputationCosts.containsKey(task)) {
			return this.averageComputationCosts.get(task);
		}

		Double averageComputationCost = computeAverageComputationCost(task);
		this.averageComputationCosts.put(task, averageComputationCost);
		return averageComputationCost;
	}

	public Double computeAverageComputationCost(Integer task) {
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
			if (!TaskUtils.canExecuteTaskOnResourceWithLimitedMemoryCapacity(computeTaskInputData(task), resourceType)) {
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

	public Double getAverageCommunicationCost(Pair<Integer, Integer> tasksDependency) {
		Double communicationCostsSum = 0.0;
		int costCount = 0;

		Integer fromTask = tasksDependency.getKey();
		Integer toTask = tasksDependency.getValue();

		for (Map.Entry<Integer, ResourceType> resourceEntry1 : this.resources.entrySet()) {
			Integer resource1 = resourceEntry1.getKey();
			ResourceType resourceType1 = resourceEntry1.getValue();
			Double resourceAvailabilityTime1 = this.resourcesAvailability.get(resource1);

			if (resourceAvailabilityTime1 > Constants.DEFAULT_RESOURCE_AVAILABILITY_TIME) {
				// Resource not available yet.
				continue;
			}
			if (!TaskUtils.canExecuteTaskOnResourceWithLimitedMemoryCapacity(computeTaskInputData(fromTask), resourceType1)) {
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
				if (!TaskUtils.canExecuteTaskOnResourceWithLimitedMemoryCapacity(computeTaskInputData(toTask), resourceType2)) {
					// Resource with limited memory capacity.
					continue;
				}

				communicationCostsSum += getCommunicationCost(tasksDependency, new Pair<Integer, Integer>(resource1, resource2));
				++costCount;
			}
		}

		return communicationCostsSum / costCount;
	}

	public Double getCommunicationCost(Pair<Integer, Integer> tasksDependency, Pair<Integer, Integer> resources) {
		Integer fromResource = resources.getKey();
		Integer toResource = resources.getValue();
		ResourceType fromResourceType = this.resources.get(fromResource);
		ResourceType toResourceType = this.resources.get(toResource);
		boolean fromEdgeResource = ResourceUtils.isEdgeResource(fromResourceType);
		boolean fromCloudResource = ResourceUtils.isCloudResource(fromResourceType);
		boolean toEdgeResource = ResourceUtils.isEdgeResource(toResourceType);
		boolean toCloudResource = ResourceUtils.isCloudResource(toResourceType);

		Integer fromTask = tasksDependency.getKey();
		Integer toTask = tasksDependency.getValue();
		Double dataDependency = getDataDependency(tasksDependency);

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

}
