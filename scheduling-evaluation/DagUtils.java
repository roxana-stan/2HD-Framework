package scheduling_evaluation;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import javafx.util.Pair;

import org.cloudbus.cloudsim.Log;

import scheduling_evaluation.Types.ResourceType;

public class DagUtils {

	// Globally modifiable variables.
	private static int taskCount = Constants.INVALID_RESULT_INT;
	private static int taskSubgraphCount = Constants.INVALID_RESULT_INT;
	private static String taskSubgraphsFilename = null;

	public static int getTaskCount() {
		return taskCount;
	}

	public static void setTaskCount(int count) {
		taskCount = count;
	}

	public static int getTaskSubgraphCount() {
		return taskSubgraphCount;
	}

	public static void setTaskSubgraphCount(int count) {
		taskSubgraphCount = count;
	}

	public static String getTaskSubgraphsFilename() {
		return taskSubgraphsFilename;
	}

	public static void setTaskSubgraphsFilename(String filename) {
		taskSubgraphsFilename = filename;
	}

	public static TaskGraph loadTaskGraph(String filename) {
		TaskGraph taskGraph = new TaskGraph();

		// Load the task graph data from the file.
		try {
			Scanner scanner = new Scanner(new File(filename));

			Pair<Map<Integer, ResourceType>, Map<Integer, Double>> resourcesWithAvailabilityInfo = DagEntityCreator.createResourcesWithAvailabilityInfo();
			Map<Integer, ResourceType> resources = resourcesWithAvailabilityInfo.getKey();
			Map<Integer, Double> resourcesAvailability = resourcesWithAvailabilityInfo.getValue();
			taskGraph.initializeResources(resources, resourcesAvailability);

			int taskCount = Integer.parseInt(scanner.nextLine());
			for (int taskIdx = 1; taskIdx <= taskCount; ++taskIdx) {
				String[] taskStrings = scanner.nextLine().split(" ");
				int taskStringIdx = 0;
				Integer task = Integer.parseInt(taskStrings[taskStringIdx++]);
				Map<ResourceType, Double> resourceComputationCosts = new HashMap<ResourceType, Double>(Constants.RESOURCE_TYPE_COUNT);
				for (ResourceType resourceType : ResourceType.values()) {
					Double resourceComputationCost = Double.parseDouble(taskStrings[taskStringIdx++]);
					resourceComputationCosts.put(resourceType, resourceComputationCost);
				}

				taskGraph.addTask(task);
				taskGraph.addTaskComputationCosts(task, resourceComputationCosts);
			}

			Integer entryTask = Integer.parseInt(scanner.nextLine());
			taskGraph.addEntryTask(entryTask);

			Integer exitTask = Integer.parseInt(scanner.nextLine());
			taskGraph.addExitTask(exitTask);

			int dependencyCount = Integer.parseInt(scanner.nextLine());
			for (int dependencyIdx = 1; dependencyIdx <= dependencyCount; ++dependencyIdx) {
				String[] dependencyStrings = scanner.nextLine().split(" ");
				int dependencyStringIdx = 0;
				Integer fromTask = Integer.parseInt(dependencyStrings[dependencyStringIdx++]);
				Integer toTask = Integer.parseInt(dependencyStrings[dependencyStringIdx++]);
				Pair<Integer, Integer> dependentTasks = new Pair<Integer, Integer>(fromTask, toTask);
				Double dataDependency = Double.parseDouble(dependencyStrings[dependencyStringIdx++]);

				taskGraph.addDependency(dependentTasks, dataDependency);
			}

			scanner.close();
		} catch (FileNotFoundException e) {
			Log.printLine("Exception in loadTaskGraph() - File not found: " + filename);
			return null;
		}

		return taskGraph;
	}

	public static List<Integer> loadTaskSubgraph(String filename, TaskGraph taskGraph) {
		List<Integer> tasks = new LinkedList<Integer>();

		// Load the task subgraph data from the file and add it to the task graph.
		try {
			Scanner scanner = new Scanner(new File(filename));

			int taskCount = Integer.parseInt(scanner.nextLine());
			for (int taskIdx = 1; taskIdx <= taskCount; ++taskIdx) {
				String[] taskStrings = scanner.nextLine().split(" ");
				int taskStringIdx = 0;
				Integer task = Integer.parseInt(taskStrings[taskStringIdx++]);
				Map<ResourceType, Double> resourceComputationCosts = new HashMap<ResourceType, Double>(Constants.RESOURCE_TYPE_COUNT);
				for (ResourceType resourceType : ResourceType.values()) {
					Double resourceComputationCost = Double.parseDouble(taskStrings[taskStringIdx++]);
					resourceComputationCosts.put(resourceType, resourceComputationCost);
				}

				taskGraph.addTask(task);
				taskGraph.addTaskComputationCosts(task, resourceComputationCosts);

				tasks.add(task);
			}

			int dependencyCount = Integer.parseInt(scanner.nextLine());
			for (int dependencyIdx = 1; dependencyIdx <= dependencyCount; ++dependencyIdx) {
				String[] dependencyStrings = scanner.nextLine().split(" ");
				int dependencyStringIdx = 0;
				Integer fromTask = Integer.parseInt(dependencyStrings[dependencyStringIdx++]);
				Integer toTask = Integer.parseInt(dependencyStrings[dependencyStringIdx++]);
				Pair<Integer, Integer> dependentTasks = new Pair<Integer, Integer>(fromTask, toTask);
				Double dataDependency = Double.parseDouble(dependencyStrings[dependencyStringIdx++]);

				taskGraph.addDependency(dependentTasks, dataDependency);
			}

			Integer exitTask = Integer.parseInt(scanner.nextLine());
			taskGraph.addExitTask(exitTask);

			scanner.close();
		} catch (FileNotFoundException e) {
			Log.printLine("Exception in loadTaskSubgraph() - File not found: " + filename);
			return null;
		}

		return tasks;
	}

}
