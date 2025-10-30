package scheduling_evaluation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import scheduling_evaluation.Types.ResourceType;

public class TaskGraphGenerator {

	public static TaskGraph generateTaskGraph(String taskGraphFilename) {
		TaskGraph taskGraph = new TaskGraph();

		int taskCount = 60;
		Integer pseudoEntryTask = 0;
		Integer pseudoExitTask = taskCount + 1;

		List<Integer> tasks = new LinkedList<Integer>();
		for (int taskId = 1; taskId <= taskCount; ++taskId) {
			tasks.add(taskId);
		}

		// Construct task graph and save it locally.
		try {
			File fout = new File(taskGraphFilename);
			FileOutputStream fos = new FileOutputStream(fout);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

			// Resources information.
			Pair<Map<Integer, ResourceType>, Map<Integer, Double>> resourcesWithAvailabilityInfo = DagEntityCreator.createResourcesWithAvailabilityInfo();
			Map<Integer, ResourceType> resources = resourcesWithAvailabilityInfo.getKey();
			Map<Integer, Double> resourcesAvailability = resourcesWithAvailabilityInfo.getValue();
			taskGraph.initializeResources(resources, resourcesAvailability);

			// Tasks information.
			bw.write("" + (taskCount+2));
			bw.newLine();

			bw.write("" + pseudoEntryTask + " " + 0.0 + " " + 0.0 + " " + 0.0);
			bw.newLine();

			for (Integer task : tasks) {
				Double taskComputationCost = 10.0;

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
			}

			bw.write("" + pseudoExitTask + " " + 0.0 + " " + 0.0 + " " + 0.0);
			bw.newLine();

			bw.write("" + pseudoEntryTask);
			bw.newLine();
			bw.write("" + pseudoExitTask);
			bw.newLine();

			// Dependencies information.
			bw.write("" + 90);	// bw.write("" + (2 * taskCount));
			bw.newLine();

			for (Integer taskId : tasks) {
				if (taskId <= 30) {
					bw.write(pseudoEntryTask + " " + taskId + " " + 0.0);
					bw.newLine();
				}
				if (taskId > 30) {
					bw.write((taskId - 30) + " " + taskId + " " + 0.0);
					bw.newLine();

					bw.write(taskId + " " + pseudoExitTask + " " + 0.0);
					bw.newLine();
				}
			}

			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

		return taskGraph;
	}

	public static void main(String[] args) {
		generateTaskGraph("data/dag/custom/dag-w30-h2-v11.txt");
	}
}
