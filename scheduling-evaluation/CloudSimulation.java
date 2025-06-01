package scheduling_evaluation;

import java.util.Calendar;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;

import scheduling_evaluation.Types.BrokerType;
import scheduling_evaluation.Types.DatacenterType;

public class CloudSimulation {

	public static void createCloudOnlySimulation(BrokerType brokerType) {
		// Initialize the CloudSim library.
		CloudSim.init(Constants.USER_COUNT, Calendar.getInstance(), false);

		// Create one cloud datacenter.
		int datacenterIdx = 1;

		String cloudDatacenterName = EntityCreator.getDatacenterName("" + datacenterIdx++, DatacenterType.CLOUD_DATACENTER);
		Datacenter cloudDatacenter = EntityCreator.createCloudDatacenter(cloudDatacenterName, Constants.CLOUD_RESOURCE_COUNT);

		// Create broker.
		int brokerIdx = 1;
		DatacenterBroker broker = EntityCreator.createBroker("" + brokerIdx, brokerType);
		int brokerId = broker.getId();

		// Create tasks and submit them to broker.
		int taskFirstId = 1;
		double[] taskArrivalTimes = SimulationUtils.loadTaskArrivalTimes(Constants.TASK_COUNT);
		List<Task> tasks = EntityCreator.createTasks(brokerId, taskFirstId, taskArrivalTimes);
		broker.submitCloudletList(tasks);

		// Create cloud resources and submit them to broker.
		int resourceFirstId = 1;
		List<Vm> cloudResources = EntityCreator.createCloudResources(brokerId, Constants.CLOUD_RESOURCE_COUNT, resourceFirstId);

		List<? extends Vm> resources = cloudResources;
		broker.submitVmList(resources);

		// Start the simulation.
		CloudSim.startSimulation();

		// Stop the simulation.
		CloudSim.stopSimulation();

		// Print results when the simulation is over.
		List<Cloudlet> executedTasks = broker.getCloudletReceivedList();
		SimulationUtils.printTasks(executedTasks);

		List<Cloudlet> notExecutedTasks = broker.getCloudletList();
		SimulationUtils.printTasks(notExecutedTasks);
		if (!notExecutedTasks.isEmpty()) {
			Log.printLine("Broker " + brokerType + " failed to schedule " + notExecutedTasks.size() + " tasks!");
		}

		// Collect and print evaluation metrics.
		SchedulingMetrics.collectMetrics(executedTasks);

		// Datacenter resource utilization.
		SchedulingMetrics.computeResourceUtilization(executedTasks, cloudDatacenter);

		// Successfully executed and failed tasks.
		SchedulingMetrics.printTaskFailureStatistics(executedTasks, notExecutedTasks);
	}

}
