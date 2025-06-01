package scheduling_evaluation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Log;

import scheduling_evaluation.DagSchedulingMetrics.DagMetricType;
import scheduling_evaluation.TaskSubgraphGenerator.TaskSubgraphGeneratorType;
import scheduling_evaluation.Types.DagBrokerType;
import scheduling_evaluation.Types.SchedulingMode;
import scheduling_evaluation.Types.WorkflowType;

public class DagExperimentsExecutor {

	// 26 Epigenomics workflows (https://github.com/wfcommons/WfInstances/tree/main/pegasus/epigenomics).
	private static final List<String> EPIGENOMICS_WORKFLOWS = Arrays.asList(
			// Epigenomics workflows (hep dataset).
			"epigenomics-chameleon-hep-1seq-100k-001",		// 43 tasks
			"epigenomics-chameleon-hep-1seq-50k-001",		// 75 tasks
			"epigenomics-chameleon-hep-2seq-100k-001",		// 121 tasks
			"epigenomics-chameleon-hep-2seq-50k-001",		// 225 tasks
			"epigenomics-chameleon-hep-3seq-100k-001",		// 235 tasks
			"epigenomics-chameleon-hep-3seq-50k-001",		// 447 tasks
			"epigenomics-chameleon-hep-4seq-100k-001",		// 349 tasks
			"epigenomics-chameleon-hep-4seq-50k-001",		// 673 tasks
			"epigenomics-chameleon-hep-5seq-100k-001",		// 423 tasks
			"epigenomics-chameleon-hep-5seq-50k-001",		// 819 tasks
			"epigenomics-chameleon-hep-6seq-100k-001",		// 509 tasks
			"epigenomics-chameleon-hep-6seq-50k-001",		// 985 tasks
			"epigenomics-chameleon-hep-7seq-100k-001",		// 579 tasks
			"epigenomics-chameleon-hep-7seq-50k-001",		// 1123 tasks
			// Epigenomics workflows (ilmn dataset).
			"epigenomics-chameleon-ilmn-1seq-100k-001",		// 127 tasks
			"epigenomics-chameleon-ilmn-1seq-50k-001",		// 243 tasks
			"epigenomics-chameleon-ilmn-2seq-100k-001",		// 265 tasks
			"epigenomics-chameleon-ilmn-2seq-50k-001",		// 517 tasks
			"epigenomics-chameleon-ilmn-3seq-100k-001",		// 407 tasks
			"epigenomics-chameleon-ilmn-3seq-50k-001",		// 795 tasks
			"epigenomics-chameleon-ilmn-4seq-100k-001",		// 561 tasks
			"epigenomics-chameleon-ilmn-4seq-50k-001",		// 1097 tasks
			"epigenomics-chameleon-ilmn-5seq-100k-001",		// 715 tasks
			"epigenomics-chameleon-ilmn-5seq-50k-001",		// 1399 tasks
			"epigenomics-chameleon-ilmn-6seq-100k-001",		// 865 tasks
			"epigenomics-chameleon-ilmn-6seq-50k-001"		// 1697 tasks
	);

	// 17 Montage workflows (https://github.com/wfcommons/WfInstances/tree/main/pegasus/montage).
	private static final List<String> MONTAGE_WORKFLOWS = Arrays.asList(
			// Montage workflows (2mass dataset).
			"montage-chameleon-2mass-005d-001",				// 60 tasks
			"montage-chameleon-2mass-015d-001",				// 312 tasks
			"montage-chameleon-2mass-01d-001",				// 105 tasks
			"montage-chameleon-2mass-025d-001",				// 621 tasks
			"montage-chameleon-2mass-02d-001",				// 621 tasks
			"montage-chameleon-2mass-03d-001",				// 750 tasks
			"montage-chameleon-2mass-04d-001",				// 1314 tasks
			"montage-chameleon-2mass-05d-001",				// 1740 tasks
			"montage-chameleon-2mass-10d-001",				// 4848 tasks
			"montage-chameleon-2mass-15d-001",				// 7119 tasks
			"montage-chameleon-2mass-20d-001",				// 9807 tasks
			// Montage workflows (dss dataset).
			"montage-chameleon-dss-05d-001",				// 60 tasks
			"montage-chameleon-dss-075d-001",				// 180 tasks
			"montage-chameleon-dss-10d-001",				// 474 tasks
			"montage-chameleon-dss-125d-001",				// 1068 tasks
			"montage-chameleon-dss-15d-001",				// 2124 tasks
			"montage-chameleon-dss-20d-001"					// 6450 tasks
	);

	private static List<String> getWorkflows(WorkflowType workflowType) {
		switch (workflowType) {
		case EPIGENOMICS: {
			return EPIGENOMICS_WORKFLOWS;
		}
		case MONTAGE: {
			return MONTAGE_WORKFLOWS;
		}
		}

		return null;
	}

	private static String getWorkflowsDirectory(WorkflowType workflowType) {
		return "data/workflows/pegasus/" + workflowType.toString().toLowerCase() + "/";
	}

	private static String getTaskGraphsDirectory(WorkflowType workflowType) {
		return "data/dag/pegasus/" + workflowType.toString().toLowerCase() + "/";
	}

	private static String getDagMetricsFilename(WorkflowType workflowType) {
		String workflowTypeString = workflowType.toString().toLowerCase();
		return "data/dag-experiments/pegasus/" + workflowTypeString + "/"
				+ workflowTypeString + "_dag_metrics_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date())
				+ Constants.FILE_EXTENSION_CSV;
	}

	private static void generateTaskGraphs(List<String> workflows, WorkflowType workflowType) {
		String workflowsDirectory = getWorkflowsDirectory(workflowType);
		String taskGraphsDirectory = getTaskGraphsDirectory(workflowType);

		for (String workflow : workflows) {
			String workflowInstanceFilename = workflowsDirectory + workflow + Constants.FILE_EXTENSION_JSON;
			String taskGraphFilename = taskGraphsDirectory + workflow + "-dag" + Constants.FILE_EXTENSION_TXT;

			TaskGraph generatedTaskGraph = DagEntityCreator.createPegasusTaskGraph(workflowInstanceFilename, taskGraphFilename, false);
			if (generatedTaskGraph != null) {
				Log.printLine(workflow + " -> " + generatedTaskGraph.getTaskCount() + " tasks");
			}
		}
	}

	private static void generateTaskSubgraphs(List<String> workflows, WorkflowType workflowType, int taskSubgraphCount) {
		String taskGraphsDirectory = getTaskGraphsDirectory(workflowType);

		for (String workflow : workflows) {
			String taskGraphFilenamePrefix = taskGraphsDirectory + workflow + "-dag";
			String taskGraphFilename = taskGraphFilenamePrefix + Constants.FILE_EXTENSION_TXT;

			TaskGraph taskGraph = DagUtils.loadTaskGraph(taskGraphFilename);
			if (taskGraph == null) {
				Log.printLine(workflow + " -> Cannot load task graph " + taskGraphFilename);
				continue;
			}

			TaskSubgraphGenerator.generateTaskSubgraphs(TaskSubgraphGeneratorType.TASK_LEVEL_GENERATOR, taskGraph, taskSubgraphCount,
					taskGraphFilenamePrefix + "-taskgen");
			TaskSubgraphGenerator.generateTaskSubgraphs(TaskSubgraphGeneratorType.TASK_SUBGRAPH_LEVEL_GENERATOR, taskGraph, taskSubgraphCount,
					taskGraphFilenamePrefix + "-tasksubgraphgen");
		}
	}

	private static void printTaskGraphs(List<String> workflows, WorkflowType workflowType) {
		String taskGraphsDirectory = getTaskGraphsDirectory(workflowType);

		for (String workflow : workflows) {
			Log.printLine("====== " + workflow + " ======");

			String taskGraphFilename = taskGraphsDirectory + workflow + "-dag" + Constants.FILE_EXTENSION_TXT;

			TaskGraph taskGraph = DagUtils.loadTaskGraph(taskGraphFilename);
			if (taskGraph == null) {
				Log.printLine(workflow + " -> Cannot load task graph " + taskGraphFilename);
				continue;
			}

			taskGraph.printTaskGraph();
		}
	}

	private static void scheduleWorkflows(List<String> workflows, WorkflowType workflowType, String dagMetricsFilename,
											int taskSubgraphCountMin, int taskSubgraphCountMax, int executionCount) {
		String dagMetricsCsvHeader = "workflow,scheduling_mode,scheduling_algorithm,execution_count,task_count,task_subgraph_count,resource_count,"
										+ "makespan,utility,utility_time_10,utility_time_20,utility_time_30,utility_time_40,utility_time_50,utility_time_60,"
										+ "utility_time_70,utility_time_75,utility_time_80,utility_time_85,utility_time_90,utility_time_95,scheduling_time";

		List<String> dagMetricsCsvRows = new LinkedList<String>();

		// Static DAG task scheduling.
		DagUtils.setTaskSubgraphCount(0);
		dagMetricsCsvRows.addAll(scheduleWorkflows(workflows, workflowType, SchedulingMode.STATIC, executionCount));

		// Dynamic DAG task scheduling.
		for (int taskSubgraphCount = taskSubgraphCountMin; taskSubgraphCount <= taskSubgraphCountMax; taskSubgraphCount += 5) {
			DagUtils.setTaskSubgraphCount(taskSubgraphCount);
			dagMetricsCsvRows.addAll(scheduleWorkflows(workflows, workflowType, SchedulingMode.DYNAMIC, executionCount));
		}

		try {
			File fout = new File(dagMetricsFilename);
			FileOutputStream fos = new FileOutputStream(fout);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

			bw.write(dagMetricsCsvHeader);
			bw.newLine();

			for (String dagMetricsCsvRow : dagMetricsCsvRows) {
				bw.write(dagMetricsCsvRow);
				bw.newLine();
			}

			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static List<String> scheduleWorkflows(List<String> workflows, WorkflowType workflowType, SchedulingMode schedulingMode, int executionCount) {
		List<String> dagMetricsCsvRows = new LinkedList<String>();

		String taskGraphsDirectory = getTaskGraphsDirectory(workflowType);

		for (String workflow : workflows) {
			Log.printLine("====== " + workflow + " -> " + schedulingMode.toString().toLowerCase() + " ======");

			if (schedulingMode == SchedulingMode.STATIC) {
				dagMetricsCsvRows.addAll(scheduleWorkflow(workflow, workflowType, schedulingMode, "static", executionCount));
			}

			if (schedulingMode == SchedulingMode.DYNAMIC) {
				DagUtils.setTaskSubgraphsFilename(taskGraphsDirectory + workflow + "-dag" + "-taskgen" + "-tasksubgraphs" + Constants.FILE_EXTENSION_TXT);
				dagMetricsCsvRows.addAll(scheduleWorkflow(workflow, workflowType, schedulingMode, "dynamic-taskgen", executionCount));

				DagUtils.setTaskSubgraphsFilename(taskGraphsDirectory + workflow + "-dag" + "-tasksubgraphgen" + "-tasksubgraphs" + Constants.FILE_EXTENSION_TXT);
				dagMetricsCsvRows.addAll(scheduleWorkflow(workflow, workflowType, schedulingMode, "dynamic-tasksubgraphgen", executionCount));
			}
		}

		return dagMetricsCsvRows;
	}

	private static List<String> scheduleWorkflow(String workflow, WorkflowType workflowType,
													SchedulingMode schedulingMode, String schedulingModeDescription,
													int executionCount) {
		// Return the collected DAG metrics for each scheduling algorithm, formatted as CSV rows.
		List<String> dagMetricsCsvRows = new LinkedList<String>();

		DecimalFormat dft = new DecimalFormat("###.##");

		String taskGraphsDirectory = getTaskGraphsDirectory(workflowType);
		String taskGraphFilename = taskGraphsDirectory + workflow + "-dag" + Constants.FILE_EXTENSION_TXT;

		for (DagBrokerType dagBrokerType : DagBrokerType.values()) {
			Map<DagMetricType, Double> dagMetrics = DagSimulation.executeSchedulingAlgorithm(dagBrokerType, schedulingMode, taskGraphFilename, executionCount, false);

			Double makespan = Constants.INVALID_RESULT_DOUBLE;
			Double utility = Constants.INVALID_RESULT_DOUBLE;
			Double utilityTime10 = Constants.INVALID_RESULT_DOUBLE;
			Double utilityTime20 = Constants.INVALID_RESULT_DOUBLE;
			Double utilityTime30 = Constants.INVALID_RESULT_DOUBLE;
			Double utilityTime40 = Constants.INVALID_RESULT_DOUBLE;
			Double utilityTime50 = Constants.INVALID_RESULT_DOUBLE;
			Double utilityTime60 = Constants.INVALID_RESULT_DOUBLE;
			Double utilityTime70 = Constants.INVALID_RESULT_DOUBLE;
			Double utilityTime75 = Constants.INVALID_RESULT_DOUBLE;
			Double utilityTime80 = Constants.INVALID_RESULT_DOUBLE;
			Double utilityTime85 = Constants.INVALID_RESULT_DOUBLE;
			Double utilityTime90 = Constants.INVALID_RESULT_DOUBLE;
			Double utilityTime95 = Constants.INVALID_RESULT_DOUBLE;
			Double schedulingTime = Constants.INVALID_RESULT_DOUBLE;
			if (dagMetrics != null) {
				makespan = dagMetrics.get(DagMetricType.MAKESPAN);
				utility = dagMetrics.get(DagMetricType.UTILITY);
				utilityTime10 = dagMetrics.get(DagMetricType.UTILITY_TIME_10);
				utilityTime20 = dagMetrics.get(DagMetricType.UTILITY_TIME_20);
				utilityTime30 = dagMetrics.get(DagMetricType.UTILITY_TIME_30);
				utilityTime40 = dagMetrics.get(DagMetricType.UTILITY_TIME_40);
				utilityTime50 = dagMetrics.get(DagMetricType.UTILITY_TIME_50);
				utilityTime60 = dagMetrics.get(DagMetricType.UTILITY_TIME_60);
				utilityTime70 = dagMetrics.get(DagMetricType.UTILITY_TIME_70);
				utilityTime75 = dagMetrics.get(DagMetricType.UTILITY_TIME_75);
				utilityTime80 = dagMetrics.get(DagMetricType.UTILITY_TIME_80);
				utilityTime85 = dagMetrics.get(DagMetricType.UTILITY_TIME_85);
				utilityTime90 = dagMetrics.get(DagMetricType.UTILITY_TIME_90);
				utilityTime95 = dagMetrics.get(DagMetricType.UTILITY_TIME_95);
				schedulingTime = dagMetrics.get(DagMetricType.SCHEDULING_TIME);
			}

			String dagMetricsCsvRow = workflow + "," + schedulingModeDescription + "," + dagBrokerType + ","
										+ executionCount + ","
										+ DagUtils.getTaskCount() + "," + DagUtils.getTaskSubgraphCount() + ","
										+ Constants.RESOURCE_COUNT + ","
										+ dft.format(makespan) + ","
										+ dft.format(utility) + ","
										+ dft.format(utilityTime10) + "," + dft.format(utilityTime20) + ","
										+ dft.format(utilityTime30) + "," + dft.format(utilityTime40) + ","
										+ dft.format(utilityTime50) + "," + dft.format(utilityTime60) + ","
										+ dft.format(utilityTime70) + "," + dft.format(utilityTime75) + ","
										+ dft.format(utilityTime80) + "," + dft.format(utilityTime85) + ","
										+ dft.format(utilityTime90) + "," + dft.format(utilityTime95) + ","
										+ dft.format(schedulingTime);
			Log.printLine(dagMetricsCsvRow);

			dagMetricsCsvRows.add(dagMetricsCsvRow);
		}

		return dagMetricsCsvRows;
	}

	public static void main(String[] args) {
		/* Configuration parameters. */
		// Default parameters.
		boolean generateResourcesConfig = false;
		boolean generateTaskGraphsConfig = false;
		boolean printTaskGraphsConfig = false;
		boolean generateTaskSubgraphArrivalTimesConfig = false;
		boolean generateTaskSubgraphsConfig = false;
		boolean executeSchedulingAlgorithmsConfig = false;
		WorkflowType workflowType = WorkflowType.EPIGENOMICS;
		List<String> workflows = getWorkflows(workflowType);
		String dagMetricsFilename = getDagMetricsFilename(workflowType);
		int taskSchedulingExecutionCount = 10;
		int taskSubgraphCountMin = 5;
		int taskSubgraphCountMax = 20;
		// Command-line parameters.
		if (args.length == 6) {
			generateTaskGraphsConfig = Boolean.parseBoolean(args[0]);
			generateTaskSubgraphsConfig = Boolean.parseBoolean(args[1]);
			executeSchedulingAlgorithmsConfig = Boolean.parseBoolean(args[2]);
			// Workflow.
			String workflow = args[3];
			// DAG metrics filename.
			dagMetricsFilename = args[4];
			// Task scheduling execution count.
			taskSchedulingExecutionCount = Integer.parseInt(args[5]);

			System.out.println("DAG experiments parameters:"
						+ " generate task graph=" + generateTaskGraphsConfig
						+ " generate task subgraphs=" + generateTaskSubgraphsConfig
						+ " execute task scheduling algorithms=" + executeSchedulingAlgorithmsConfig
						+ " workflow=" + workflow
						+ " DAG metrics filename=" + dagMetricsFilename
						+ " task scheduling execution count=" + taskSchedulingExecutionCount);

			if (workflow.startsWith("epigenomics")) {
				workflowType = WorkflowType.EPIGENOMICS;
			} else if (workflow.startsWith("montage")) {
				workflowType = WorkflowType.MONTAGE;
			} else {
				System.out.println("Invalid workflow: " + workflow);
				return;
			}
			workflows = Arrays.asList(workflow);
		}

		/* Resource generation. */
		if (generateResourcesConfig) {
			SimulationUtils.generateResourceAvailabilityTimes();
		}

		/* Task graph generation. */
		if (generateTaskGraphsConfig) {
			generateTaskGraphs(workflows, workflowType);
		}
		if (printTaskGraphsConfig) {
			printTaskGraphs(workflows, workflowType);
		}

		/* Task subgraph generation. */
		if (generateTaskSubgraphArrivalTimesConfig) {
			SimulationUtils.generateTaskSubgraphArrivalTimes(taskSubgraphCountMax);
		}
		if (generateTaskSubgraphsConfig) {
			generateTaskSubgraphs(workflows, workflowType, taskSubgraphCountMax);
		}

		/* DAG task scheduling. */
		if (executeSchedulingAlgorithmsConfig) {
			scheduleWorkflows(workflows, workflowType, dagMetricsFilename, taskSubgraphCountMin, taskSubgraphCountMax, taskSchedulingExecutionCount);
		}
	}

}
