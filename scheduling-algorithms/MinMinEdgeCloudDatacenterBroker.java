package scheduling_algorithms;

import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;

import scheduling_evaluation.Task;
import scheduling_evaluation.TaskUtils;
import scheduling_evaluation.Types.TaskExecutionResourceStatus;

public class MinMinEdgeCloudDatacenterBroker extends DefaultEdgeCloudDatacenterBroker {

	private int taskCount = 0;
	private int resourceCount = 0;

	private double[] readyTimeArray;
	private double[][] executionTimeMatrix;
	private double[][] completionTimeMatrix;

	public MinMinEdgeCloudDatacenterBroker(String name) throws Exception {
		super(name);
	}

	/**
	 * Submit cloudlets to the created VMs.
	 * 
	 * @pre $none
	 * @post $none
	 */
	@Override
	protected void submitCloudlets() {
		List<Cloudlet> handledCloudlets = new LinkedList<Cloudlet>();

		/* Compute number of tasks and resources. */
		taskCount = getCloudletList().size();
		resourceCount = getVmsCreatedList().size();

		/* Compute execution time E_ij, ready time R_j and completion time C_ij of task Ti on resource Rj. */
		executionTimeMatrix = new double[taskCount][resourceCount];
		initializeExecutionTimeMatrix(getCloudletList(), getVmsCreatedList());

		readyTimeArray = new double[resourceCount];
		initializeReadyTimeArray();

		completionTimeMatrix = new double[taskCount][resourceCount];
		updateCompletionTimeMatrix();
		printCompletionTimeMatrix(getCloudletList(), handledCloudlets, getVmsCreatedList());	// Debugging.

		/* MinMin algorithm: do while there are unallocated tasks. */
		while (handledCloudlets.size() != taskCount) {
			int taskIdx = -1;
			int resourceIdx = -1;
			double minCompletionTime = Double.MAX_VALUE;

			/* For each task, find the minimum completion time and the resource that obtains it. */
			for (int i = 0; i < taskCount; ++i) {
				Cloudlet currentCloudlet = getCloudletList().get(i);
				if (handledCloudlets.contains(currentCloudlet)) {	// Task is already handled.
					continue;
				}

				double minResourceCompletionTime = Double.MAX_VALUE;
				int minResourceIdx = -1;
				for (int j = 0; j < resourceCount; ++j) {
					if (completionTimeMatrix[i][j] < minResourceCompletionTime) {
						minResourceCompletionTime = completionTimeMatrix[i][j];
						minResourceIdx = j;
					}
				}

				/* Select the task having the minimum completion time among all unassigned tasks. */
				if (minResourceCompletionTime < minCompletionTime) {
					minCompletionTime = minResourceCompletionTime;
					taskIdx = i;
					resourceIdx = minResourceIdx;
				}
			}

			/* Allocate the task to the resource that obtains the selected task's completion time. */
			Cloudlet cloudlet = getCloudletList().get(taskIdx);
			Vm vm = getVmsCreatedList().get(resourceIdx);

			handledCloudlets.add(cloudlet);

			Task task = (Task) cloudlet;
			TaskExecutionResourceStatus resourceStatus = canExecuteTaskOnResource(cloudlet, vm);
			task.setResourceStatus(resourceStatus);

			if (resourceStatus != TaskExecutionResourceStatus.SUCCESS) {
				Log.printLine(CloudSim.clock() + ": " + getName() + ": Cannot send cloudlet " + cloudlet.getCloudletId() + " to VM #" + vm.getId()
							+ " - Error: " + resourceStatus);
				continue;
			}

			// Update the current capacity of the edge device's battery.
			updateEdgeDeviceBattery(cloudlet, vm);

			// Update the task's length such that the processing time on the selected resource includes the data transfer time.
			task.setTotalExecutionTime(TaskUtils.getTotalExecutionTime(cloudlet, vm));
			TaskUtils.setCloudletTotalLength(cloudlet, vm);

			Log.printLine(CloudSim.clock() + ": " + getName() + ": Sending cloudlet " + cloudlet.getCloudletId() + " to VM #" + vm.getId());
			cloudlet.setVmId(vm.getId());
			send(getVmsToDatacentersMap().get(vm.getId()), task.getArrivalTime(), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
			cloudletsSubmitted++;
			getCloudletSubmittedList().add(cloudlet);

			// Update the ready time of the selected resource.
			readyTimeArray[resourceIdx] = minCompletionTime;

			// Update the tasks' completion time.
			updateCompletionTimeMatrix();
			/*
			printCompletionTimeMatrix(getCloudletList(), handledCloudlets, getVmsCreatedList());	// Debugging.
			*/
		}

		// remove submitted cloudlets from waiting list
		for (Cloudlet cloudlet : getCloudletSubmittedList()) {
			getCloudletList().remove(cloudlet);
		}
	}

	private void initializeExecutionTimeMatrix(List<Cloudlet> cloudlets, List<Vm> vms) {
		for (int i = 0; i < taskCount; ++i) {
			Cloudlet cloudlet = cloudlets.get(i);

			for (int j = 0; j < resourceCount; ++j) {
				Vm vm = vms.get(j);

				executionTimeMatrix[i][j] = TaskUtils.getTotalExecutionTime(cloudlet, vm);
			}
		}
	}

	private void initializeReadyTimeArray() {
		for (int j = 0; j < resourceCount; ++j) {
			readyTimeArray[j] = 0.0;
		}
	}

	private void updateCompletionTimeMatrix() {
		for (int i = 0; i < taskCount; ++i) {
			for (int j = 0; j < resourceCount; ++j) {
				completionTimeMatrix[i][j] = executionTimeMatrix[i][j] + readyTimeArray[j];
			}
		}
	}

	private void printCompletionTimeMatrix(List<Cloudlet> cloudlets, List<Cloudlet> handledCloudlets, List<Vm> vms) {
		for (int i = 0; i < taskCount; ++i) {
			Task task = (Task) cloudlets.get(i);
			if (handledCloudlets.contains(task)) {			// Task is already handled.
				continue;
			}

			Log.print("Task " + task.getCloudletId() + ", type " + task.getType() + " - ");
			for (int j = 0; j < resourceCount; ++j) {
				Vm vm = vms.get(j);
				Log.print("VM #" + vm.getId() + ": " + completionTimeMatrix[i][j] + "  ");
			}
			Log.printLine();
		}
	}

}
