package scheduling_evaluation;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import scheduling_evaluation.Types.ResourceType;

public class HeftData {

	// Tasks.
	private static final int TASK_COUNT											= 10;
	private static final List<Integer> TASKS									= initializeTasks();

	private static final List<Integer> ENTRY_TASKS								= new LinkedList<Integer>(Arrays.asList(1));
	private static final List<Integer> EXIT_TASKS								= new LinkedList<Integer>(Arrays.asList(10));

	// Dependencies.
	private static final Map<Pair<Integer, Integer>, Double> DEPENDENCIES		= initializeDependencies();

	// Resources.
	private static final Pair<Map<Integer, ResourceType>, Map<Integer, Double>> RESOURCES_WITH_AVAILABILITY_INFO	= initializeResourcesWithAvailabilityInfo();

	// Computation costs.
	private static final double[][] COMPUTATION_COSTS_DATA						= new double[][] {
		{ 14, 16,  9 },
		{ 13, 19, 18 },
		{ 11, 13, 19 },
		{ 13,  8, 17 },
		{ 12, 13, 10 },
		{ 13, 16,  9 },
		{  7, 15, 11 },
		{  5, 11, 14 },
		{ 18, 12, 20 },
		{ 21,  7, 16 }
	};
	private static final Map<Integer, Map<ResourceType, Double>> COMPUTATION_COSTS	= initializeComputationCosts();

	public HeftData() {
	}

	private static List<Integer> initializeTasks() {
		List<Integer> tasks = new LinkedList<Integer>();

		for (int taskIdx = 0; taskIdx < TASK_COUNT; ++taskIdx) {
			Integer taskId = taskIdx + 1;
			tasks.add(taskId);
		}

		return Collections.unmodifiableList(tasks);
	}

	private static Map<Pair<Integer, Integer>, Double> initializeDependencies() {
		/*
		 * LinkedHashMap maintains the insertion order of elements.
		 * Used only for debugging purposes, but the code works correctly if HashMap is used instead.
		 */
		Map<Pair<Integer, Integer>, Double> dependencies = new LinkedHashMap<Pair<Integer, Integer>, Double>();

		dependencies.put(new Pair<Integer, Integer>(1,  2), 18.0);
		dependencies.put(new Pair<Integer, Integer>(1,  3), 12.0);
		dependencies.put(new Pair<Integer, Integer>(1,  4),  9.0);
		dependencies.put(new Pair<Integer, Integer>(1,  5), 11.0);
		dependencies.put(new Pair<Integer, Integer>(1,  6), 14.0);
		dependencies.put(new Pair<Integer, Integer>(2,  8), 19.0);
		dependencies.put(new Pair<Integer, Integer>(2,  9), 16.0);
		dependencies.put(new Pair<Integer, Integer>(3,  7), 23.0);
		dependencies.put(new Pair<Integer, Integer>(4,  8), 27.0);
		dependencies.put(new Pair<Integer, Integer>(4,  9), 23.0);
		dependencies.put(new Pair<Integer, Integer>(5,  9), 13.0);
		dependencies.put(new Pair<Integer, Integer>(6,  8), 15.0);
		dependencies.put(new Pair<Integer, Integer>(7, 10), 17.0);
		dependencies.put(new Pair<Integer, Integer>(8, 10), 11.0);
		dependencies.put(new Pair<Integer, Integer>(9, 10), 13.0);

		return Collections.unmodifiableMap(dependencies);
	}

	private static Pair<Map<Integer, ResourceType>, Map<Integer, Double>> initializeResourcesWithAvailabilityInfo() {
		return DagEntityCreator.createResourcesWithAvailabilityInfo();
	}

	private static Map<Integer, Map<ResourceType, Double>> initializeComputationCosts() {
		Map<Integer, Map<ResourceType, Double>> computationCosts = new HashMap<Integer, Map<ResourceType, Double>>(TASK_COUNT);

		int taskIdx = 0;
		for (Integer task : TASKS) {
			computationCosts.put(task, new HashMap<ResourceType, Double>(Constants.RESOURCE_TYPE_COUNT));

			int resourceIdx = 0;
			for (ResourceType resourceType : ResourceType.values()) { 
				computationCosts.get(task).put(resourceType, COMPUTATION_COSTS_DATA[taskIdx][resourceIdx]);
				++resourceIdx;
			}

			++taskIdx;
		}

		return Collections.unmodifiableMap(computationCosts);
	}

	public int getTaskCount() {
		return TASK_COUNT;
	}

	public List<Integer> getTasks() {
		return TASKS;
	}

	public List<Integer> getEntryTasks() {
		return ENTRY_TASKS;
	}

	public List<Integer> getExitTasks() {
		return EXIT_TASKS;
	}

	public Map<Pair<Integer, Integer>, Double> getDependencies() {
		return DEPENDENCIES;
	}

	public Pair<Map<Integer, ResourceType>, Map<Integer, Double>> getResourcesWithAvailabilityInfo() {
		return RESOURCES_WITH_AVAILABILITY_INFO;
	}

	public Map<Integer, Map<ResourceType, Double>> getComputationCosts() {
		return COMPUTATION_COSTS;
	}

}
