package scheduling_evaluation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.PatternSyntaxException;

import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import dag_scheduling_algorithms.CpopEdgeCloudDatacenterBroker;
import dag_scheduling_algorithms.HeftEdgeCloudDatacenterBroker;
import dag_scheduling_algorithms.PetsEdgeCloudDatacenterBroker;
import dag_scheduling_algorithms.PredeterminedScheduleEdgeCloudDatacenterBroker;
import dag_scheduling_algorithms.RandHeftEdgeCloudDatacenterBroker;
import dag_scheduling_algorithms.RandUtilityEdgeCloudDatacenterBroker;
import dag_scheduling_algorithms.UtilityEdgeCloudDatacenterBroker;
import dag_scheduling_algorithms.dynamic.DynamicCpopEdgeCloudDatacenterBroker;
import dag_scheduling_algorithms.dynamic.DynamicHeftEdgeCloudDatacenterBroker;
import dag_scheduling_algorithms.dynamic.DynamicPetsEdgeCloudDatacenterBroker;
import dag_scheduling_algorithms.dynamic.DynamicPredeterminedScheduleEdgeCloudDatacenterBroker;
import dag_scheduling_algorithms.dynamic.DynamicRandHeftEdgeCloudDatacenterBroker;
import dag_scheduling_algorithms.dynamic.DynamicRandUtilityEdgeCloudDatacenterBroker;
import dag_scheduling_algorithms.dynamic.DynamicUtilityEdgeCloudDatacenterBroker;
import scheduling_evaluation.Types.DagBrokerType;
import scheduling_evaluation.Types.ResourceType;
import scheduling_evaluation.Types.SchedulingMode;

public class DagEntityCreator {

	/**
	 * Creates a DatacenterBroker of the indicated type.
	 * @param nameSuffix Broker name suffix, added to the broker type string constant.
	 * @param type Broker type, implementing a specific dependent task scheduling algorithm.
	 * @param taskGraph DAG configuration.
	 * @param schedulingMode Static or dynamic functioning mode of the scheduling routine.
	 * @return The created DatacenterBroker object.
	 */
	public static DatacenterBroker createDagBroker(String nameSuffix, DagBrokerType type, TaskGraph taskGraph, SchedulingMode schedulingMode) {
		DatacenterBroker broker = null;

		try {
			String brokerName = Types.dagBrokerTypeString(type) + nameSuffix;

			switch (schedulingMode) {
			case STATIC: {
				broker = createStaticDagBroker(brokerName, type, taskGraph);
				break;
			}
			case DYNAMIC: {
				broker = createDynamicDagBroker(brokerName, type, taskGraph);
				break;
			}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

		return broker;
	}

	private static DatacenterBroker createStaticDagBroker(String brokerName, DagBrokerType type, TaskGraph taskGraph) {
		DatacenterBroker broker = null;

		try {
			switch (type) {
			case EDGE_CLOUD_HEFT_BROKER: {
				broker = new HeftEdgeCloudDatacenterBroker(brokerName, taskGraph);
				break;
			}
			case EDGE_CLOUD_RAND_HEFT_BROKER: {
				broker = new RandHeftEdgeCloudDatacenterBroker(brokerName, taskGraph);
				break;
			}
			case EDGE_CLOUD_QL_HEFT_BROKER: {
				broker = new PredeterminedScheduleEdgeCloudDatacenterBroker(brokerName, taskGraph, taskGraph.getQlHeftSchedule());
				break;
			}
			case EDGE_CLOUD_CPOP_BROKER: {
				broker = new CpopEdgeCloudDatacenterBroker(brokerName, taskGraph);
				break;
			}
			case EDGE_CLOUD_PETS_BROKER: {
				broker = new PetsEdgeCloudDatacenterBroker(brokerName, taskGraph);
				break;
			}
			case EDGE_CLOUD_2HD_BROKER: {
				broker = new UtilityEdgeCloudDatacenterBroker(brokerName, taskGraph, /* hybrid= */ false);
				break;
			}
			case EDGE_CLOUD_RAND_2HD_BROKER: {
				broker = new RandUtilityEdgeCloudDatacenterBroker(brokerName, taskGraph, /* hybrid= */ false);
				break;
			}
			case EDGE_CLOUD_QL_2HD_BROKER: {
				broker = new PredeterminedScheduleEdgeCloudDatacenterBroker(brokerName, taskGraph, taskGraph.getQl2hdSchedule());
				break;
			}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

		return broker;
	}

	private static DatacenterBroker createDynamicDagBroker(String brokerName, DagBrokerType type, TaskGraph taskGraph) {
		DatacenterBroker broker = null;

		try {
			switch (type) {
			case EDGE_CLOUD_HEFT_BROKER: {
				broker = new DynamicHeftEdgeCloudDatacenterBroker(brokerName, taskGraph);
				break;
			}
			case EDGE_CLOUD_RAND_HEFT_BROKER: {
				broker = new DynamicRandHeftEdgeCloudDatacenterBroker(brokerName, taskGraph);
				break;
			}
			case EDGE_CLOUD_QL_HEFT_BROKER: {
				broker = new DynamicPredeterminedScheduleEdgeCloudDatacenterBroker(brokerName, taskGraph, taskGraph.getQlHeftSchedule());
				break;
			}
			case EDGE_CLOUD_CPOP_BROKER: {
				broker = new DynamicCpopEdgeCloudDatacenterBroker(brokerName, taskGraph);
				break;
			}
			case EDGE_CLOUD_PETS_BROKER: {
				broker = new DynamicPetsEdgeCloudDatacenterBroker(brokerName, taskGraph);
				break;
			}
			case EDGE_CLOUD_2HD_BROKER: {
				broker = new DynamicUtilityEdgeCloudDatacenterBroker(brokerName, taskGraph, /* hybrid= */ false);
				break;
			}
			case EDGE_CLOUD_RAND_2HD_BROKER: {
				broker = new DynamicRandUtilityEdgeCloudDatacenterBroker(brokerName, taskGraph, /* hybrid= */ false);
				break;
			}
			case EDGE_CLOUD_QL_2HD_BROKER: {
				broker = new DynamicPredeterminedScheduleEdgeCloudDatacenterBroker(brokerName, taskGraph, taskGraph.getQl2hdSchedule());
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
	 * Creates a DAG of execution tasks and their dependencies (precedence constraints, communication data).
	 * @param workflowInstanceFilename Name of the file storing the workflow instance data.
	 * @param taskGraphFilename Name of the file storing the created task graph data.
	 * @param verboseMode Verbose mode.
	 * @return The created task graph.
	 */
	public static TaskGraph createPegasusTaskGraph(String workflowInstanceFilename, String taskGraphFilename, boolean verboseMode) {
		TaskGraph taskGraph = new TaskGraph();

		// Temporary data used for creating the task graph based on the workflow data.
		HashMap<String, Long> fileMappings = new HashMap<String, Long>();								// File size <B>
		HashMap<String, HashSet<String>> inputFileTasksMappings = new HashMap<String, HashSet<String>>();
		HashMap<String, HashSet<String>> outputFileTasksMappings = new HashMap<String, HashSet<String>>();

		Map<String, Integer> taskInfoMappings = new HashMap<String, Integer>();
		Map<String, Double> taskComputationTimeMappings = new HashMap<String, Double>();
		Map<Pair<String, String>, Long> dependencies = new HashMap<Pair<String, String>, Long>();		// Data <B>

		/* Parse the workflow instance file represented in the WfCommons JSON format: WfFormat (https://github.com/wfcommons/wfformat). */

		// Add zero-cost pseudo entry and exit tasks, considering data dependencies as well.
		Integer pseudoEntryTask = Constants.INVALID_RESULT_INT;
		Integer pseudoExitTask = Constants.INVALID_RESULT_INT;
		String pseudoEntryTaskId = "pseudo_entry_task";
		String pseudoExitTaskId = "pseudo_exit_task";
		Integer minTask = Integer.MAX_VALUE;
		Integer maxTask = Integer.MIN_VALUE;

		try (FileReader jsonReader = new FileReader(workflowInstanceFilename)) {
			JSONParser jsonParser = new JSONParser();
			JSONObject instanceJsonObject = (JSONObject) jsonParser.parse(jsonReader);

			JSONObject workflowJsonObject = (JSONObject) instanceJsonObject.get("workflow");

			JSONObject workflowSpecificationJsonObject = (JSONObject) workflowJsonObject.get("specification");
			JSONObject workflowExecutionJsonObject = (JSONObject) workflowJsonObject.get("execution");

			JSONArray filesSpecificationJsonArray = (JSONArray) workflowSpecificationJsonObject.get("files");
			int fileSpecificationCount = filesSpecificationJsonArray.size();
			for (int fileIdx = 0; fileIdx < fileSpecificationCount; ++fileIdx) {
				JSONObject fileSpecificationJsonObject = (JSONObject) filesSpecificationJsonArray.get(fileIdx);

				String fileId = (String) fileSpecificationJsonObject.get("id");
				Long fileSize = ((Number) fileSpecificationJsonObject.get("sizeInBytes")).longValue();

				fileMappings.put(fileId, fileSize);
			}

			JSONArray tasksSpecificationJsonArray = (JSONArray) workflowSpecificationJsonObject.get("tasks");
			JSONArray tasksExecutionJsonArray = (JSONArray) workflowExecutionJsonObject.get("tasks");

			int taskSpecificationCount = tasksSpecificationJsonArray.size();
			int taskExecutionCount = tasksExecutionJsonArray.size();
			if (taskSpecificationCount != taskExecutionCount) {
				Log.printLine("Error in createPegasusTaskGraph() - Workflow data (task count) is invalid: "
						+ taskSpecificationCount + " - " + taskExecutionCount + " | " + workflowInstanceFilename);
				return null;
			}

			for (int taskIdx = 0; taskIdx < taskSpecificationCount; ++taskIdx) {
				JSONObject taskSpecificationJsonObject = (JSONObject) tasksSpecificationJsonArray.get(taskIdx);
				JSONObject taskExecutionJsonObject = (JSONObject) tasksExecutionJsonArray.get(taskIdx);

				Double taskRuntime = ((Number) taskExecutionJsonObject.get("runtimeInSeconds")).doubleValue();

				String taskSpecificationId = (String) taskSpecificationJsonObject.get("id");
				String taskExecutionId = (String) taskExecutionJsonObject.get("id");
				if (!taskSpecificationId.equals(taskExecutionId)) {
					Log.printLine("Error in createPegasusTaskGraph() - Workflow data (task ID) is invalid: "
							+ taskSpecificationId + " - " + taskExecutionId + " | " + workflowInstanceFilename);
					return null;
				}

				String taskId = taskSpecificationId;
				String[] taskIdSplitParts = taskId.split("_ID");
				if (taskIdSplitParts.length != 2) {
					Log.printLine("Error in createPegasusTaskGraph() - Workflow data (task ID) is invalid: "
							+ taskId + " | " + workflowInstanceFilename);
					return null;
				}
				Integer task = Integer.parseInt(taskIdSplitParts[1]);

				taskInfoMappings.put(taskId, task);
				taskComputationTimeMappings.put(taskId, taskRuntime);

				minTask = Math.min(minTask, task);
				maxTask = Math.max(maxTask, task);

				JSONArray parentTasksIds = (JSONArray) taskSpecificationJsonObject.get("parents");
				int parentTaskCount = parentTasksIds.size();
				if (parentTaskCount == 0) {
					// Entry task.
					Pair<String, String> dependentTasksIds = new Pair<String, String>(pseudoEntryTaskId, taskId);
					dependencies.put(dependentTasksIds, (long) 0);
				}

				JSONArray childTasksIds = (JSONArray) taskSpecificationJsonObject.get("children");
				int childTaskCount = childTasksIds.size();
				if (childTaskCount == 0) {
					// Exit task.
					Pair<String, String> dependentTasksIds = new Pair<String, String>(taskId, pseudoExitTaskId);
					dependencies.put(dependentTasksIds, (long) 0);
				}
				for (int childTaskIdx = 0; childTaskIdx < childTaskCount; ++childTaskIdx) {
					String childTaskId = (String) childTasksIds.get(childTaskIdx);
					Pair<String, String> dependentTasksIds = new Pair<String, String>(taskId, childTaskId);
					dependencies.put(dependentTasksIds, (long) 0);
				}

				JSONArray taskInputFiles = (JSONArray) taskSpecificationJsonObject.get("inputFiles");
				int taskInputFileCount = taskInputFiles.size();
				for (int fileIdx = 0; fileIdx < taskInputFileCount; ++fileIdx) {
					String fileId = (String) taskInputFiles.get(fileIdx);
					if (!inputFileTasksMappings.containsKey(fileId)) {
						inputFileTasksMappings.put(fileId, new HashSet<String>());
					}
					inputFileTasksMappings.get(fileId).add(taskId);
				}

				JSONArray taskOutputFiles = (JSONArray) taskSpecificationJsonObject.get("outputFiles");
				int taskOutputFileCount = taskOutputFiles.size();
				for (int fileIdx = 0; fileIdx < taskOutputFileCount; ++fileIdx) {
					String fileId = (String) taskOutputFiles.get(fileIdx);
					if (!outputFileTasksMappings.containsKey(fileId)) {
						outputFileTasksMappings.put(fileId, new HashSet<String>());
					}
					outputFileTasksMappings.get(fileId).add(taskId);
				}
			}

			pseudoEntryTask = minTask - 1;
			taskInfoMappings.put(pseudoEntryTaskId, pseudoEntryTask);
			taskComputationTimeMappings.put(pseudoEntryTaskId, 0.0);

			pseudoExitTask = maxTask + 1;
			taskInfoMappings.put(pseudoExitTaskId, pseudoExitTask);
			taskComputationTimeMappings.put(pseudoExitTaskId, 0.0);
		} catch (FileNotFoundException e) {
			Log.printLine("Exception in createPegasusTaskGraph() - File not found | " + workflowInstanceFilename);
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		} catch (PatternSyntaxException e) {
			Log.printLine("Exception in createPegasusTaskGraph() - Failed to parse task ID | " + workflowInstanceFilename);
			return null;
		}

		// Compute the dependencies between tasks.
		for (String fileId : fileMappings.keySet()) {
			Long fileSize = fileMappings.get(fileId);

			HashSet<String> inputFileTasksIds = inputFileTasksMappings.get(fileId);
			HashSet<String> outputFileTasksIds = outputFileTasksMappings.get(fileId);

			if (inputFileTasksIds == null && outputFileTasksIds == null) {
				Log.printLine("File not used by tasks: " + fileId);
				continue;
			}

			if (inputFileTasksIds == null) {
				String toTaskId = pseudoExitTaskId;
				for (String fromTaskId : outputFileTasksIds) {
					Pair<String, String> dependentTasksIds = new Pair<String, String>(fromTaskId, toTaskId);
					Long dataDependency = fileSize;
					if (dependencies.containsKey(dependentTasksIds)) {
						dataDependency += dependencies.get(dependentTasksIds);
					}
					dependencies.put(dependentTasksIds, dataDependency);
					if (verboseMode) {
						Log.printLine("File (output-only): " + fileId + " | Data dependency: " + fromTaskId + " -> " + toTaskId);
					}
				}
				continue;
			}

			if (outputFileTasksIds == null) {
				String fromTaskId = pseudoEntryTaskId;
				for (String toTaskId : inputFileTasksIds) {
					Pair<String, String> dependentTasksIds = new Pair<String, String>(fromTaskId, toTaskId);
					Long dataDependency = fileSize;
					if (dependencies.containsKey(dependentTasksIds)) {
						dataDependency += dependencies.get(dependentTasksIds);
					}
					dependencies.put(dependentTasksIds, dataDependency);
					if (verboseMode) {
						Log.printLine("File (input-only): " + fileId + " | Data dependency: " + fromTaskId + " -> " + toTaskId);
					}
				}
				continue;
			}

			for (String fromTaskId : outputFileTasksIds) {
				for (String toTaskId : inputFileTasksIds) {
					Pair<String, String> dependentTasksIds = new Pair<String, String>(fromTaskId, toTaskId);
					Long dataDependency = fileSize;
					if (dependencies.containsKey(dependentTasksIds)) {
						dataDependency += dependencies.get(dependentTasksIds);
					}
					dependencies.put(dependentTasksIds, dataDependency);
					if (verboseMode) {
						Log.printLine("File (input-output): " + fileId + " | Data dependency: " + fromTaskId + " -> " + toTaskId);
					}
				}
			}
		}

		// Construct task graph and save it locally.
		try {
			File fout = new File(taskGraphFilename);
			FileOutputStream fos = new FileOutputStream(fout);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

			// Resources information.
			Pair<Map<Integer, ResourceType>, Map<Integer, Double>> resourcesWithAvailabilityInfo = createResourcesWithAvailabilityInfo();
			Map<Integer, ResourceType> resources = resourcesWithAvailabilityInfo.getKey();
			Map<Integer, Double> resourcesAvailability = resourcesWithAvailabilityInfo.getValue();
			taskGraph.initializeResources(resources, resourcesAvailability);

			// Tasks information.
			int taskCount = taskInfoMappings.size();
			bw.write("" + taskCount);
			bw.newLine();

			for (String taskId : taskInfoMappings.keySet()) {
				Integer task = taskInfoMappings.get(taskId);
				Double taskComputationCost = taskComputationTimeMappings.get(taskId);

				bw.write("" + task);
				Map<ResourceType, Double> resourceComputationCosts = new HashMap<ResourceType, Double>(Constants.RESOURCE_TYPE_COUNT);
				for (ResourceType resourceType : ResourceType.values()) {
					Double resourceComputationCost = 0.0;
					if (taskComputationCost > 0.0) {
						switch (resourceType) {
						case EDGE_RESOURCE_MOBILE_PHONE: {
							resourceComputationCost = SimulationUtils.getRandomNumber(3.0 * taskComputationCost, 5.0 * taskComputationCost);
							break;
						}
						case EDGE_RESOURCE_RASPBERRY_PI: {
							resourceComputationCost = SimulationUtils.getRandomNumber(5.0 * taskComputationCost, 10.0 * taskComputationCost);
							break;
						}
						case CLOUD_RESOURCE: {
							resourceComputationCost = taskComputationCost;
							break;
						}
						}
					}
					resourceComputationCosts.put(resourceType, resourceComputationCost);
					bw.write(" " + resourceComputationCost);
				}
				bw.newLine();

				taskGraph.addTask(task);
				taskGraph.addTaskComputationCosts(task, resourceComputationCosts);
			}

			// Entry and exit tasks information.
			taskGraph.addEntryTask(pseudoEntryTask);
			taskGraph.addExitTask(pseudoExitTask);

			bw.write("" + pseudoEntryTask);
			bw.newLine();
			bw.write("" + pseudoExitTask);
			bw.newLine();

			// Dependencies information.
			bw.write("" + dependencies.size());
			bw.newLine();

			for (Map.Entry<Pair<String, String>, Long> dependencyEntry : dependencies.entrySet()) {
				Pair<String, String> dependentTasksIds = dependencyEntry.getKey();
				Double dataDependency = dependencyEntry.getValue() / Math.pow(10, 6);		// Data <MB>

				Integer fromTask = taskInfoMappings.get(dependentTasksIds.getKey());
				Integer toTask = taskInfoMappings.get(dependentTasksIds.getValue());

				taskGraph.addDependency(fromTask, toTask, dataDependency);

				bw.write(fromTask + " " + toTask + " " + dataDependency);
				bw.newLine();
			}

			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

		return taskGraph;
	}

	/**
	 * Creates a task graph according to the configuration data considered in the HEFT and CPOP paper.
	 * @return The created task graph.
	 */
	public static TaskGraph createHeftTaskGraph() {
		TaskGraph taskGraph = new TaskGraph();

		// Get HEFT task graph data.
		HeftData data = new HeftData();

		// Add tasks.
		taskGraph.addTasks(data.getTasks());
		taskGraph.addEntryTasks(data.getEntryTasks());
		taskGraph.addExitTasks(data.getExitTasks());
		// Add dependencies.
		taskGraph.addDependencies(data.getDependencies());
		// Initialize resources.
		taskGraph.initializeResources(data.getResourcesWithAvailabilityInfo().getKey(), data.getResourcesWithAvailabilityInfo().getValue());
		// Add computation costs.
		taskGraph.addComputationCosts(data.getComputationCosts());

		return taskGraph;
	}

	/**
	 * Creates a list of DAG generic tasks.
	 * @param userId User or broker ID.
	 * @param taskIds IDs of the tasks to be created.
	 * @param taskArrivalTime Tasks' arrival time, assuming that all tasks arrive at the same time.
	 * @return The created list of generic tasks.
	 */
	public static List<Task> createGenericTasks(int userId, List<Integer> taskIds, double taskArrivalTime) {
		int taskCount = taskIds.size();
		// The actual values are set accordingly at scheduling time given the DAG configuration.
		double[] taskLengths = new double[taskCount];
		Arrays.fill(taskLengths, 0.0);
		double[] taskFileSizes = new double[taskCount];
		Arrays.fill(taskFileSizes, 0.0);
		double[] taskOutputSizes = new double[taskCount];
		Arrays.fill(taskOutputSizes, 0.0);
		double[] taskArrivalTimes = new double[taskCount];
		Arrays.fill(taskArrivalTimes, taskArrivalTime);

		List<Task> tasks = EntityCreator.createGenericTasks(userId, taskCount, taskIds,
															taskLengths, taskFileSizes, taskOutputSizes, taskArrivalTimes);
		return tasks;
	}

	public static Pair<Map<Integer, ResourceType>, Map<Integer, Double>> createResourcesWithAvailabilityInfo() {
		// RESOURCE_COUNT = SMARTPHONE_RESOURCE_COUNT + RASPBERRY_PI_RESOURCE_COUNT + CLOUD_RESOURCE_COUNT;
		Map<Integer, ResourceType> resources = new HashMap<Integer, ResourceType>(Constants.RESOURCE_COUNT);
		Map<Integer, Double> resourcesAvailability = new HashMap<Integer, Double>(Constants.RESOURCE_COUNT);

		int resourceCount = 0;
		int smartphoneResourceCount = 0;
		int raspberryPiResourceCount = 0;
		int cloudResourceCount = 0;

		Integer resourceId = 1;
		Integer smartphoneResourceId = resourceId;
		Integer raspberryPiResourceId = resourceId + Constants.SMARTPHONE_RESOURCE_COUNT;
		Integer cloudResourceId = resourceId + Constants.SMARTPHONE_RESOURCE_COUNT + Constants.RASPBERRY_PI_RESOURCE_COUNT;

		double[] resourceAvailabilityTimes = SimulationUtils.loadResourceAvailabilityTimes(Constants.RESOURCE_COUNT);
		int resourceIdx = 0;

		do {
			if (smartphoneResourceCount < Constants.SMARTPHONE_RESOURCE_COUNT) {
				resources.put(smartphoneResourceId, ResourceType.EDGE_RESOURCE_MOBILE_PHONE);
				resourcesAvailability.put(smartphoneResourceId, resourceAvailabilityTimes[resourceIdx++]);
				++smartphoneResourceId;
				++smartphoneResourceCount;
			}
			if (raspberryPiResourceCount < Constants.RASPBERRY_PI_RESOURCE_COUNT) {
				resources.put(raspberryPiResourceId, ResourceType.EDGE_RESOURCE_RASPBERRY_PI);
				resourcesAvailability.put(raspberryPiResourceId, resourceAvailabilityTimes[resourceIdx++]);
				++raspberryPiResourceId;
				++raspberryPiResourceCount;
			}
			if (cloudResourceCount < Constants.CLOUD_RESOURCE_COUNT) {
				resources.put(cloudResourceId, ResourceType.CLOUD_RESOURCE);
				resourcesAvailability.put(cloudResourceId, resourceAvailabilityTimes[resourceIdx++]);
				++cloudResourceId;
				++cloudResourceCount;
			}
			resourceCount = smartphoneResourceCount + raspberryPiResourceCount + cloudResourceCount;
		} while (resourceCount < Constants.RESOURCE_COUNT);

		return new Pair<Map<Integer, ResourceType>, Map<Integer, Double>>(
				Collections.unmodifiableMap(resources),
				Collections.unmodifiableMap(resourcesAvailability));
	}

}
