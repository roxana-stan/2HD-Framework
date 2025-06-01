package scheduling_evaluation;

import java.text.DecimalFormat;
import java.time.Duration;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.edge.core.edge.MicroELement;

import scheduling_evaluation.DagSchedulingMetrics.DagMetricType;
import scheduling_evaluation.Types.DagBrokerType;
import scheduling_evaluation.Types.DatacenterType;
import scheduling_evaluation.Types.ResourceType;
import scheduling_evaluation.Types.SchedulingMode;

public class DagSimulation {

	public static final int EXECUTION_COUNT = 1000;

	public static void main(String[] args) {
		/*
		 * Broker types:
		 * - EDGE_CLOUD_HEFT_BROKER
		 * - EDGE_CLOUD_RAND_HEFT_BROKER
		 * - EDGE_CLOUD_UTILITY_BROKER
		 * - EDGE_CLOUD_RAND_UTILITY_BROKER
		 */
		DagBrokerType dagBrokerType = DagBrokerType.EDGE_CLOUD_HEFT_BROKER;
		SchedulingMode schedulingMode = SchedulingMode.STATIC;
		String taskGraphFilename = "";
		int executionCount = EXECUTION_COUNT;
		boolean verboseMode = true;

		Map<DagMetricType, Double> dagMetrics = executeSchedulingAlgorithm(dagBrokerType, schedulingMode, taskGraphFilename, executionCount, verboseMode);
		if (dagMetrics == null) {
			Log.printLine("EDGE-CLOUD DAG simulation aborted");
			Log.printLine(dagBrokerType + " - " + schedulingMode);
			return;
		}

		Double averageMakespan = dagMetrics.get(DagMetricType.MAKESPAN);
		Double averageUtility = dagMetrics.get(DagMetricType.UTILITY);
		Double averageUtilityTime10 = dagMetrics.get(DagMetricType.UTILITY_TIME_10);
		Double averageUtilityTime20 = dagMetrics.get(DagMetricType.UTILITY_TIME_20);
		Double averageUtilityTime30 = dagMetrics.get(DagMetricType.UTILITY_TIME_30);
		Double averageUtilityTime40 = dagMetrics.get(DagMetricType.UTILITY_TIME_40);
		Double averageUtilityTime50 = dagMetrics.get(DagMetricType.UTILITY_TIME_50);
		Double averageUtilityTime60 = dagMetrics.get(DagMetricType.UTILITY_TIME_60);
		Double averageUtilityTime70 = dagMetrics.get(DagMetricType.UTILITY_TIME_70);
		Double averageUtilityTime75 = dagMetrics.get(DagMetricType.UTILITY_TIME_75);
		Double averageUtilityTime80 = dagMetrics.get(DagMetricType.UTILITY_TIME_80);
		Double averageUtilityTime85 = dagMetrics.get(DagMetricType.UTILITY_TIME_85);
		Double averageUtilityTime90 = dagMetrics.get(DagMetricType.UTILITY_TIME_90);
		Double averageUtilityTime95 = dagMetrics.get(DagMetricType.UTILITY_TIME_95);
		Double averageSchedulingTime = dagMetrics.get(DagMetricType.SCHEDULING_TIME);

		DecimalFormat dft = new DecimalFormat("###.##");
		Log.printLine("Avg. makespan (" + executionCount + " executions): " + dft.format(averageMakespan) + " seconds");
		Log.printLine("Avg. utility (" + executionCount + " executions): " + dft.format(averageUtility));
		Log.printLine("Avg. @10 utility time (" + executionCount + " executions): " + dft.format(averageUtilityTime10) + " seconds");
		Log.printLine("Avg. @20 utility time (" + executionCount + " executions): " + dft.format(averageUtilityTime20) + " seconds");
		Log.printLine("Avg. @30 utility time (" + executionCount + " executions): " + dft.format(averageUtilityTime30) + " seconds");
		Log.printLine("Avg. @40 utility time (" + executionCount + " executions): " + dft.format(averageUtilityTime40) + " seconds");
		Log.printLine("Avg. @50 utility time (" + executionCount + " executions): " + dft.format(averageUtilityTime50) + " seconds");
		Log.printLine("Avg. @60 utility time (" + executionCount + " executions): " + dft.format(averageUtilityTime60) + " seconds");
		Log.printLine("Avg. @70 utility time (" + executionCount + " executions): " + dft.format(averageUtilityTime70) + " seconds");
		Log.printLine("Avg. @75 utility time (" + executionCount + " executions): " + dft.format(averageUtilityTime75) + " seconds");
		Log.printLine("Avg. @80 utility time (" + executionCount + " executions): " + dft.format(averageUtilityTime80) + " seconds");
		Log.printLine("Avg. @85 utility time (" + executionCount + " executions): " + dft.format(averageUtilityTime85) + " seconds");
		Log.printLine("Avg. @90 utility time (" + executionCount + " executions): " + dft.format(averageUtilityTime90) + " seconds");
		Log.printLine("Avg. @95 utility time (" + executionCount + " executions): " + dft.format(averageUtilityTime95) + " seconds");
		Log.printLine("Avg. scheduling time (" + executionCount + " executions): " + dft.format(averageSchedulingTime) + " seconds");

		Log.printLine("EDGE-CLOUD DAG simulation done");
		Log.printLine(dagBrokerType + " - " + schedulingMode);
	}

	public static Map<DagMetricType, Double> executeSchedulingAlgorithm(DagBrokerType dagBrokerType, SchedulingMode schedulingMode, String taskGraphFilename, int executionCount, boolean verboseMode) {
		Double makespanSum = 0.0;
		Double utilitySum = 0.0;
		Double utilityTime10Sum = 0.0;
		Double utilityTime20Sum = 0.0;
		Double utilityTime30Sum = 0.0;
		Double utilityTime40Sum = 0.0;
		Double utilityTime50Sum = 0.0;
		Double utilityTime60Sum = 0.0;
		Double utilityTime70Sum = 0.0;
		Double utilityTime75Sum = 0.0;
		Double utilityTime80Sum = 0.0;
		Double utilityTime85Sum = 0.0;
		Double utilityTime90Sum = 0.0;
		Double utilityTime95Sum = 0.0;
		Duration totalSchedulingTimeDuration = Duration.ZERO;

		for (int execution = 1; execution <= executionCount; ++execution) {
			Map<DagMetricType, Double> executionDagMetrics = createEdgeCloudDagSimulation(dagBrokerType, schedulingMode, taskGraphFilename, verboseMode);
			if (executionDagMetrics == null) {
				Log.printLine("Broker " + dagBrokerType + " - Error in execution " + execution + " / " + executionCount + ". Aborting...");
				return null;
			}
			makespanSum += executionDagMetrics.get(DagMetricType.MAKESPAN);
			utilitySum += executionDagMetrics.get(DagMetricType.UTILITY);
			utilityTime10Sum += executionDagMetrics.get(DagMetricType.UTILITY_TIME_10);
			utilityTime20Sum += executionDagMetrics.get(DagMetricType.UTILITY_TIME_20);
			utilityTime30Sum += executionDagMetrics.get(DagMetricType.UTILITY_TIME_30);
			utilityTime40Sum += executionDagMetrics.get(DagMetricType.UTILITY_TIME_40);
			utilityTime50Sum += executionDagMetrics.get(DagMetricType.UTILITY_TIME_50);
			utilityTime60Sum += executionDagMetrics.get(DagMetricType.UTILITY_TIME_60);
			utilityTime70Sum += executionDagMetrics.get(DagMetricType.UTILITY_TIME_70);
			utilityTime75Sum += executionDagMetrics.get(DagMetricType.UTILITY_TIME_75);
			utilityTime80Sum += executionDagMetrics.get(DagMetricType.UTILITY_TIME_80);
			utilityTime85Sum += executionDagMetrics.get(DagMetricType.UTILITY_TIME_85);
			utilityTime90Sum += executionDagMetrics.get(DagMetricType.UTILITY_TIME_90);
			utilityTime95Sum += executionDagMetrics.get(DagMetricType.UTILITY_TIME_95);
			totalSchedulingTimeDuration = totalSchedulingTimeDuration.plus(DagSchedulingMetrics.getSchedulingTimeDuration());
		}

		Map<DagMetricType, Double> dagMetrics = new HashMap<DagMetricType, Double>();
		dagMetrics.put(DagMetricType.MAKESPAN, makespanSum / executionCount);
		dagMetrics.put(DagMetricType.UTILITY, utilitySum / executionCount);
		dagMetrics.put(DagMetricType.UTILITY_TIME_10, utilityTime10Sum / executionCount);
		dagMetrics.put(DagMetricType.UTILITY_TIME_20, utilityTime20Sum / executionCount);
		dagMetrics.put(DagMetricType.UTILITY_TIME_30, utilityTime30Sum / executionCount);
		dagMetrics.put(DagMetricType.UTILITY_TIME_40, utilityTime40Sum / executionCount);
		dagMetrics.put(DagMetricType.UTILITY_TIME_50, utilityTime50Sum / executionCount);
		dagMetrics.put(DagMetricType.UTILITY_TIME_60, utilityTime60Sum / executionCount);
		dagMetrics.put(DagMetricType.UTILITY_TIME_70, utilityTime70Sum / executionCount);
		dagMetrics.put(DagMetricType.UTILITY_TIME_75, utilityTime75Sum / executionCount);
		dagMetrics.put(DagMetricType.UTILITY_TIME_80, utilityTime80Sum / executionCount);
		dagMetrics.put(DagMetricType.UTILITY_TIME_85, utilityTime85Sum / executionCount);
		dagMetrics.put(DagMetricType.UTILITY_TIME_90, utilityTime90Sum / executionCount);
		dagMetrics.put(DagMetricType.UTILITY_TIME_95, utilityTime95Sum / executionCount);
		dagMetrics.put(DagMetricType.SCHEDULING_TIME, DagSchedulingMetrics.getTime(totalSchedulingTimeDuration) / executionCount);

		return dagMetrics;
	}

	private static Map<DagMetricType, Double> createEdgeCloudDagSimulation(DagBrokerType dagBrokerType, SchedulingMode schedulingMode, String taskGraphFilename, boolean verboseMode) {
		// Enable or disable the simulation logs based on the verbose mode parameter.
		Log.setDisabled(!verboseMode);

		// Initialize the CloudSim library.
		CloudSim.init(Constants.USER_COUNT, Calendar.getInstance(), false);

		// Create edge and cloud datacenters.
		int datacenterIdx = 1;

		String edgeDatacenterNameM = EntityCreator.getDatacenterName("" + datacenterIdx++, DatacenterType.EDGE_DATACENTER);
		Datacenter edgeDatacenterM = EdgeEntityCreator.createEdgeDatacenter(edgeDatacenterNameM, Constants.SMARTPHONE_RESOURCE_COUNT,
																			ResourceType.EDGE_RESOURCE_MOBILE_PHONE);

		String edgeDatacenterNameR = EntityCreator.getDatacenterName("" + datacenterIdx++, DatacenterType.EDGE_DATACENTER);
		Datacenter edgeDatacenterR = EdgeEntityCreator.createEdgeDatacenter(edgeDatacenterNameR, Constants.RASPBERRY_PI_RESOURCE_COUNT,
																			ResourceType.EDGE_RESOURCE_RASPBERRY_PI);

		String cloudDatacenterName = EntityCreator.getDatacenterName("" + datacenterIdx++, DatacenterType.CLOUD_DATACENTER);
		Datacenter cloudDatacenter = EntityCreator.createCloudDatacenter(cloudDatacenterName, Constants.CLOUD_RESOURCE_COUNT);

		// Create DAG.
		TaskGraph taskGraph = DagUtils.loadTaskGraph(taskGraphFilename);
		if (taskGraph == null) {
			return null;
		}

		// Create broker.
		int brokerIdx = 1;
		DatacenterBroker broker = DagEntityCreator.createDagBroker("" + brokerIdx, dagBrokerType, taskGraph, schedulingMode);
		int brokerId = broker.getId();

		// Create edge and cloud resources and submit them to broker.
		int resourceFirstId = 1;
		List<MicroELement> edgeResourcesM = EdgeEntityCreator.createEdgeResources(brokerId, Constants.SMARTPHONE_RESOURCE_COUNT, resourceFirstId,
																				ResourceType.EDGE_RESOURCE_MOBILE_PHONE);
		resourceFirstId += Constants.SMARTPHONE_RESOURCE_COUNT;
		List<MicroELement> edgeResourcesR = EdgeEntityCreator.createEdgeResources(brokerId, Constants.RASPBERRY_PI_RESOURCE_COUNT, resourceFirstId,
																				ResourceType.EDGE_RESOURCE_RASPBERRY_PI);
		resourceFirstId += Constants.RASPBERRY_PI_RESOURCE_COUNT;
		List<Vm> cloudResources = EntityCreator.createCloudResources(brokerId, Constants.CLOUD_RESOURCE_COUNT, resourceFirstId);

		List<? extends Vm> resources = Stream.concat(Stream.concat(edgeResourcesM.stream(), edgeResourcesR.stream()), cloudResources.stream())
											.collect(Collectors.toList());
		broker.submitVmList(resources);

		// Create tasks and submit them to broker.
		List<Integer> entryTaskIds = taskGraph.getEntryTasks();
		if (entryTaskIds.size() != 1) {
			// Assume that the task graph has a single entry task.
			return null;
		}
		int taskFirstId = entryTaskIds.get(0);
		List<Task> tasks = DagEntityCreator.createGenericTasks(brokerId, taskFirstId, taskGraph.getTaskCount(), Constants.DEFAULT_TASK_ARRIVAL_TIME);
		broker.submitCloudletList(tasks);

		// Start the simulation.
		CloudSim.startSimulation();

		// Stop the simulation.
		CloudSim.stopSimulation();

		// Enable the logs when the simulation is over.
		Log.setDisabled(false);

		// Collect the results when the simulation is over.
		List<Cloudlet> executedTasks = broker.getCloudletReceivedList();
		if (verboseMode) {
			SimulationUtils.printTasks(executedTasks);
		}

		List<Cloudlet> notExecutedTasks = broker.getCloudletList();
		if (verboseMode) {
			SimulationUtils.printTasks(notExecutedTasks);
		}
		if (!notExecutedTasks.isEmpty()) {
			Log.printLine("Broker " + dagBrokerType + " failed to schedule " + notExecutedTasks.size() + " tasks!");
			return null;
		}

		// DAG scheduling evaluation metrics.
		Map<DagMetricType, Double> dagSchedulingMetrics = DagSchedulingMetrics.collectMetrics(executedTasks, taskGraph, verboseMode);

		if (verboseMode) {
			// Scheduling evaluation metrics.
			SchedulingMetrics.collectMetrics(executedTasks);

			// Datacenter resource utilization.
			SchedulingMetrics.computeResourceUtilization(executedTasks, edgeDatacenterM);
			SchedulingMetrics.computeResourceUtilization(executedTasks, edgeDatacenterR);
			SchedulingMetrics.computeResourceUtilization(executedTasks, cloudDatacenter);

			// Energy consumption for edge devices.
			SimulationUtils.printEdgeDevices(edgeDatacenterM);
			SimulationUtils.printEdgeDevices(edgeDatacenterR);

			// Successfully executed and failed tasks.
			SchedulingMetrics.printTaskFailureStatistics(executedTasks, notExecutedTasks);
		}

		return dagSchedulingMetrics;
	}

}
