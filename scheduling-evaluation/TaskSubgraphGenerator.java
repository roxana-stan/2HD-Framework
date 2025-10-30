package scheduling_evaluation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.cloudbus.cloudsim.Log;

import scheduling_evaluation.Types.ResourceType;

public class TaskSubgraphGenerator {

	public enum TaskSubgraphGeneratorType {
		TASK_LEVEL_GENERATOR,
		TASK_SUBGRAPH_LEVEL_GENERATOR;
	}

	private static Map<Integer, Integer> newTaskToOriginalTaskMappings = new HashMap<Integer, Integer>();

	private static Integer taskOffset = Constants.INVALID_RESULT_INT;
	private static Integer nextTask = Constants.INVALID_RESULT_INT;

	public static void generateTaskSubgraphs(TaskSubgraphGeneratorType generatorType, String taskGraphFilename,
											int taskSubgraphCount, String taskGraphFilenamePrefix) {
		TaskGraph taskGraph = DagUtils.loadTaskGraph(taskGraphFilename);
		if (taskGraph == null) {
			Log.printLine("Cannot load task graph " + taskGraphFilename);
			return;
		}
		if (taskGraph.getEntryTasks().size() != 1 || taskGraph.getExitTasks().size() != 1) {
			Log.printLine("Task graph should have 1 pseudo-entry task and 1 pseudo-exit task " + taskGraphFilename);
			return;
		}

		// Generate the task subgraphs and write the information to the output file.
		try {
			File fout = new File(taskGraphFilenamePrefix + "-tasksubgraphs" + Constants.FILE_EXTENSION_TXT);
			FileOutputStream fos = new FileOutputStream(fout);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

			for (int taskSubgraphIdx = 1; taskSubgraphIdx <= taskSubgraphCount; ++taskSubgraphIdx) {
				// Write the task subgraph related information to the output file.
				String taskSubgraphFilename = taskGraphFilenamePrefix + "-tasksubgraph" + taskSubgraphIdx + Constants.FILE_EXTENSION_TXT;
				bw.write(taskSubgraphFilename);
				bw.newLine();

				// Generate the task subgraph and write the data to the corresponding task subgraph output file.
				Log.printLine("====== Generate task subgraph " + taskSubgraphIdx + " ======");
				taskOffset = 10000 * taskSubgraphIdx;
				nextTask = taskOffset + 1;
				generateTaskSubgraph(generatorType, taskSubgraphFilename, taskGraph, taskGraphFilename);
			}

			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void generateTaskSubgraph(TaskSubgraphGeneratorType generatorType, String taskSubgraphFilename,
											TaskGraph taskGraph, String taskGraphFilename) {
		/* Generate the task subgraph data. */
		List<Integer> tasks = new LinkedList<Integer>();
		Map<Pair<Integer, Integer>, Double> dependencies = new HashMap<Pair<Integer, Integer>, Double>();
		List<Integer> exitTasks = new LinkedList<Integer>();

		// Task subgraph generation parameters.
		Random random = new Random();
		int subgraphWidth = Math.max(3, random.nextInt(6));
		int subgraphHeight = Math.max(3, random.nextInt(11));
		Log.printLine("------ Parameters: task subgraph width " + subgraphWidth + ", task subgraph height " + subgraphHeight + " ------");

		// Generate the subgraph's tasks.
		if (generatorType == TaskSubgraphGeneratorType.TASK_SUBGRAPH_LEVEL_GENERATOR) {
			generateTasksViaTaskSubgraphLevelGenerator(subgraphWidth, subgraphHeight, taskGraph, tasks, dependencies, exitTasks);
		}
		if (generatorType == TaskSubgraphGeneratorType.TASK_LEVEL_GENERATOR) {
			generateTasksViaTaskLevelGenerator(subgraphWidth, subgraphHeight, taskGraph, tasks, dependencies, exitTasks);
		}

		/* Save the generated task subgraph data to the file. */
		try {
			File fout = new File(taskSubgraphFilename);
			FileOutputStream fos = new FileOutputStream(fout);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

			// Tasks information.
			bw.write("" + tasks.size());
			bw.newLine();

			for (Integer task : tasks) {
				bw.write("" + task);
				Integer originalTask = newTaskToOriginalTaskMappings.get(task);
				Map<ResourceType, Double> resourceComputationCosts = taskGraph.getComputationCosts().get(originalTask);
				for (ResourceType resourceType : ResourceType.values()) {
					Double resourceComputationCost = resourceComputationCosts.get(resourceType);
					bw.write(" " + resourceComputationCost);
				}
				bw.newLine();
			}

			// Dependencies information.
			bw.write("" + dependencies.size());
			bw.newLine();

			for (Map.Entry<Pair<Integer, Integer>, Double> dependencyEntry : dependencies.entrySet()) {
				Pair<Integer, Integer> dependentTasks = dependencyEntry.getKey();
				Double dataDependency = dependencyEntry.getValue();
				Integer fromTask = dependentTasks.getKey();
				Integer toTask = dependentTasks.getValue();
				bw.write(fromTask + " " + toTask + " " + dataDependency);
				bw.newLine();
			}

			// Exit task information.
			Integer exitTask = exitTasks.get(0);
			bw.write("" + exitTask);
			bw.newLine();

			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void generateTasksViaTaskSubgraphLevelGenerator(int subgraphWidth, int subgraphHeight, TaskGraph taskGraph,
																List<Integer> tasks, Map<Pair<Integer, Integer>, Double> dependencies, List<Integer> exitTasks) {
		// Randomly select a task from the original task graph.
		Integer randomTask = getRandomTask(taskGraph);
		List<Integer> currentLevelTasks = new LinkedList<Integer>(Arrays.asList(randomTask));

		// Generate the subgraph's tasks.
		while (subgraphHeight > 0) {
			--subgraphHeight;

			Log.printLine("------ Current level tasks: " + currentLevelTasks.toString());
			List<Integer> nextLevelTasks = new LinkedList<Integer>();

			// Probability 5%: Do not create tasks.
			double randomNumber = Math.random();
			if (randomNumber < 0.05) {
				Log.printLine("> 0 tasks generated");
				Log.printLine("------ Next level tasks: " + currentLevelTasks.toString());
				Log.printLine();
				continue;
			}
			// Probability 15%: Next level tasks are merge tasks.
			else if (randomNumber < 0.20) {
				List<Integer> mergeParentTasks = currentLevelTasks;
				Log.printLine("> 1 merge task generated - parent tasks: " + mergeParentTasks.toString());
				generateNextTask(mergeParentTasks, taskGraph, nextLevelTasks, dependencies);
			}
			// Probability 25%: Next level tasks are split tasks.
			else if (randomNumber < 0.45) {
				for (Integer currentLevelTask : currentLevelTasks) {
					int splitTaskCount = subgraphWidth;
					List<Integer> splitParentTasks = new LinkedList<Integer>(Arrays.asList(currentLevelTask));
					Log.printLine("> " + splitTaskCount + " split tasks generated - parent tasks: " + splitParentTasks.toString());
					while (splitTaskCount > 0) {
						generateNextTask(splitParentTasks, taskGraph, nextLevelTasks, dependencies);
						--splitTaskCount;
					}
				}
			}
			// Probability 55%: Next level tasks are map tasks.
			else if (randomNumber < 1.0) {
				for (Integer currentLevelTask : currentLevelTasks) {
					List<Integer> mapParentTasks = new LinkedList<Integer>(Arrays.asList(currentLevelTask));
					Log.printLine("> 1 map task generated - parent tasks: " + mapParentTasks.toString());
					generateNextTask(mapParentTasks, taskGraph, nextLevelTasks, dependencies);
				}
			}

			tasks.addAll(nextLevelTasks);

			currentLevelTasks.clear();
			currentLevelTasks.addAll(nextLevelTasks);
			Log.printLine("------ Next level tasks: " + currentLevelTasks.toString());
			Log.printLine();
		}

		// Generate the subgraph's exit task.
		Integer exitTask = generateExitTask(currentLevelTasks, taskGraph, tasks, dependencies);
		exitTasks.add(exitTask);
	}

	private static void generateTasksViaTaskLevelGenerator(int subgraphWidth, int subgraphHeight, TaskGraph taskGraph,
														List<Integer> tasks, Map<Pair<Integer, Integer>, Double> dependencies, List<Integer> exitTasks) {
		// Randomly select a task from the original task graph.
		Integer randomTask = getRandomTask(taskGraph);
		List<Integer> currentLevelTasks = new LinkedList<Integer>(Arrays.asList(randomTask));

		// Generate the subgraph's tasks.
		while (subgraphHeight > 0) {
			--subgraphHeight;

			Log.printLine("------ Current level tasks: " + currentLevelTasks.toString());
			List<Integer> nextLevelExistingTasks = new LinkedList<Integer>();
			List<Integer> nextLevelTasks = new LinkedList<Integer>();

			for (Integer currentLevelTask : currentLevelTasks) {
				// Probability 5%: Do not create tasks.
				double randomNumber = Math.random();
				if (randomNumber < 0.05) {
					Log.printLine("> 0 tasks generated");
					nextLevelExistingTasks.add(currentLevelTask);
				}
				// Probability 15%: Next task is a merge task.
				else if (randomNumber < 0.20) {
					int mergeParentTaskCount = subgraphWidth;
					List<Integer> mergeParentTasks = new LinkedList<Integer>(Arrays.asList(currentLevelTask));
					--mergeParentTaskCount;
					while (mergeParentTaskCount > 0) {
						Integer mergeParentRandomTask = getRandomTask(taskGraph);
						if (mergeParentTasks.contains(mergeParentRandomTask)) {
							continue;
						}
						mergeParentTasks.add(mergeParentRandomTask);
						--mergeParentTaskCount;
					}
					Log.printLine("> 1 merge task generated - parent tasks: " + mergeParentTasks.toString());
					generateNextTask(mergeParentTasks, taskGraph, nextLevelTasks, dependencies);
				}
				// Probability 25%: Next tasks are split tasks.
				else if (randomNumber < 0.45) {
					int splitTaskCount = subgraphWidth;
					List<Integer> splitParentTasks = new LinkedList<Integer>(Arrays.asList(currentLevelTask));
					Log.printLine("> " + splitTaskCount + " split tasks generated - parent tasks: " + splitParentTasks.toString());
					while (splitTaskCount > 0) {
						generateNextTask(splitParentTasks, taskGraph, nextLevelTasks, dependencies);
						--splitTaskCount;
					}
				}
				// Probability 55%: Next task is a map task.
				else if (randomNumber < 1.0) {
					List<Integer> mapParentTasks = new LinkedList<Integer>(Arrays.asList(currentLevelTask));
					Log.printLine("> 1 map task generated - parent tasks: " + mapParentTasks.toString());
					generateNextTask(mapParentTasks, taskGraph, nextLevelTasks, dependencies);
				}
			}

			tasks.addAll(nextLevelTasks);

			currentLevelTasks.clear();
			currentLevelTasks.addAll(nextLevelExistingTasks);
			currentLevelTasks.addAll(nextLevelTasks);
			Log.printLine("------ Next level tasks: " + currentLevelTasks.toString());
			Log.printLine();
		}

		// Generate the subgraph's exit task.
		Integer exitTask = generateExitTask(currentLevelTasks, taskGraph, tasks, dependencies);
		exitTasks.add(exitTask);
	}

	private static Integer generateExitTask(List<Integer> parentTasks, TaskGraph taskGraph,
											List<Integer> tasks, Map<Pair<Integer, Integer>, Double> dependencies) {
		Integer exitTask = nextTask;

		Log.printLine("> (exit) 1 merge task generated - parent tasks: " + parentTasks.toString());
		generateNextTask(parentTasks, taskGraph, tasks, dependencies);

		return exitTask;
	}

	private static void generateNextTask(List<Integer> parentTasks, TaskGraph taskGraph,
										List<Integer> tasks, Map<Pair<Integer, Integer>, Double> dependencies) {
		Log.printLine("  -> task: " + nextTask);

		Pair<Double, Double> dataDependencyLimits = taskGraph.computeDataDependencyLimits();
		Double minDataDependency = dataDependencyLimits.getKey();
		Double maxDataDependency = dataDependencyLimits.getValue();

		Integer randomTask = getRandomTask(taskGraph);
		newTaskToOriginalTaskMappings.put(nextTask, randomTask);

		tasks.add(nextTask);

		for (Integer parentTask : parentTasks) {
			Pair<Integer, Integer> dependentTasks = new Pair<Integer, Integer>(parentTask, nextTask);
			Double dataDependency = SimulationUtils.getRandomRoundedNumber(minDataDependency, maxDataDependency);

			dependencies.put(dependentTasks, dataDependency);
		}

		++nextTask;
	}

	private static Integer getRandomTask(TaskGraph taskGraph) {
		Integer entryTask = taskGraph.getEntryTasks().get(0);
		int taskCount = taskGraph.getTaskCount();

		// Randomly select a task from the original task graph.
		// In the original task graph, the tasks are consecutive numbers.
		// Exclude the pseudo entry / exit tasks.
		Random random = new Random();
		Integer randomTask = Constants.INVALID_RESULT_INT;
		do {
			randomTask = entryTask + random.nextInt(taskCount);
		} while (taskGraph.isEntryTask(randomTask) || taskGraph.isExitTask(randomTask));

		return randomTask;
	}

	public static void main(String[] args) {
		TaskGraph taskGraph = DagEntityCreator.createHeftTaskGraph();
		int taskSubgraphCount = 2 * taskGraph.getTaskCount() / 10;
		String taskGraphFilenamePrefix = "data/dag/heft/heft-dag";

		boolean generateTaskSubgraphsConfig = false;
		if (generateTaskSubgraphsConfig) {
			Log.printLine("====== Generate task subgraphs ======");
			generateTaskSubgraphs(TaskSubgraphGeneratorType.TASK_LEVEL_GENERATOR, "", taskSubgraphCount, taskGraphFilenamePrefix);
		}

		boolean loadTaskSubgraphConfig = false;
		if (loadTaskSubgraphConfig) {
			for (int subgraphIdx = 1; subgraphIdx <= taskSubgraphCount; ++subgraphIdx) {
				Log.printLine("====== Load task subgraph " + subgraphIdx + " ======");
				String subgraphFilename = taskGraphFilenamePrefix + "-tasksubgraph" + subgraphIdx + Constants.FILE_EXTENSION_TXT;
				List<Integer> tasks = DagUtils.loadTaskSubgraph(subgraphFilename, taskGraph);

				Log.printLine("> Tasks: " + tasks.toString());
				Log.printLine("> Exit tasks: " + taskGraph.getExitTasks().toString());
			}
			taskGraph.printTaskGraph();
		}
	}

}
