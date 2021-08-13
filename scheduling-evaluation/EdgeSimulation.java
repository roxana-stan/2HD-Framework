package scheduling_evaluation;

import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.edge.core.edge.MicroELement;

import scheduling_evaluation.Types.BrokerType;
import scheduling_evaluation.Types.DatacenterType;
import scheduling_evaluation.Types.ResourceType;

public class EdgeSimulation {

	public static void createEdgeCloudSimulation(BrokerType brokerType) {
		// Initialize the CloudSim library
		CloudSim.init(Constants.USER_COUNT, Calendar.getInstance(), false);

		// Create edge and cloud datacenters
		int datacenterIdx = 1;

		String edgeDatacenterNameM = EntityCreator.getDatacenterName("" + datacenterIdx++, DatacenterType.EDGE_DATACENTER);
		Datacenter edgeDatacenterM = EdgeEntityCreator.createEdgeDatacenter(edgeDatacenterNameM, Constants.SMARTPHONE_RESOURCE_COUNT,
																			ResourceType.EDGE_RESOURCE_MOBILE_PHONE);

		String edgeDatacenterNameR = EntityCreator.getDatacenterName("" + datacenterIdx++, DatacenterType.EDGE_DATACENTER);
		Datacenter edgeDatacenterR = EdgeEntityCreator.createEdgeDatacenter(edgeDatacenterNameR, Constants.RASPBERRY_PI_RESOURCE_COUNT,
																			ResourceType.EDGE_RESOURCE_RASPBERRY_PI);

		String cloudDatacenterName = EntityCreator.getDatacenterName("" + datacenterIdx++, DatacenterType.CLOUD_DATACENTER);
		Datacenter cloudDatacenter = EntityCreator.createCloudDatacenter(cloudDatacenterName, Constants.CLOUD_RESOURCE_COUNT);

		// Create broker
		int brokerIdx = 1;
		DatacenterBroker broker = EntityCreator.createBroker("" + brokerIdx, brokerType);
		int brokerId = broker.getId();

		// Create tasks and submit them to broker
		int taskFirstId = 1;
		double[] taskArrivalTimes = SimulationUtils.loadTaskArrivalTimes(Constants.TASK_COUNT);
		List<Task> tasks = EntityCreator.createTasks(brokerId, taskFirstId, taskArrivalTimes);
		broker.submitCloudletList(tasks);

		// Create edge and cloud resources and submit them to broker
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

		// Start the simulation
		CloudSim.startSimulation();

		// Stop the simulation
		CloudSim.stopSimulation();

		// Print results when simulation is over
		List<Cloudlet> executedTasks = broker.getCloudletReceivedList();
		SimulationUtils.printTasks(executedTasks);

		List<Cloudlet> notExecutedTasks = broker.getCloudletList();
		if (!notExecutedTasks.isEmpty()) {
			SimulationUtils.printTasks(notExecutedTasks);
		}

		// Collect and print evaluation metrics
		SchedulingMetrics.collectMetrics(executedTasks);

		// Datacenter resource utilization
		SchedulingMetrics.computeResourceUtilization(executedTasks, edgeDatacenterM);
		SchedulingMetrics.computeResourceUtilization(executedTasks, edgeDatacenterR);
		SchedulingMetrics.computeResourceUtilization(executedTasks, cloudDatacenter);

		// Energy consumption for edge devices
		SimulationUtils.printEdgeDevices(edgeDatacenterM);
		SimulationUtils.printEdgeDevices(edgeDatacenterR);

		// Successfully executed and failed tasks
		SchedulingMetrics.printTaskFailureStatistics(executedTasks, notExecutedTasks);
	}

}
