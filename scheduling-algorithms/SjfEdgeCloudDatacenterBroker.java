package scheduling_algorithms;

import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.lists.VmList;

import scheduling_evaluation.Task;
import scheduling_evaluation.TaskUtils;
import scheduling_evaluation.Types.TaskExecutionResourceStatus;

public class SjfEdgeCloudDatacenterBroker extends DefaultEdgeCloudDatacenterBroker {

	public SjfEdgeCloudDatacenterBroker(String name) throws Exception {
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
		List<Vm> vms = getVmsCreatedList();

		// Sort cloudlets in ascending order of their expected execution times.
		getCloudletList().sort(new Comparator<Cloudlet>() {
			@Override
			public int compare(Cloudlet cloudlet1, Cloudlet cloudlet2) {
				Task task1 = (Task) cloudlet1;
				Task task2 = (Task) cloudlet2;
				double executionTimeTask1 = task1.getWeightedAverageExecutionTime(vms);
				double executionTimeTask2 = task2.getWeightedAverageExecutionTime(vms);
				return Double.compare(executionTimeTask1, executionTimeTask2);
			}
		});

		// Debugging: print sorted list of cloudlets.
		Log.printLine("=== SJF - Sorted list of tasks ===");
		DecimalFormat dft = new DecimalFormat("##.##");
		for (Cloudlet cloudlet : getCloudletList()) {
			Task task = (Task) cloudlet;
			double executionTime = task.getWeightedAverageExecutionTime(vms);
			Log.printLine("Task " + task.getCloudletId() + ", type " + task.getType() +  " - Expected execution time: " + dft.format(executionTime));
		}

		int vmIndex = 0;
		for (Cloudlet cloudlet : getCloudletList()) {
			Vm vm;
			// If user didn't bind this cloudlet and it has not been executed yet.
			if (cloudlet.getVmId() == -1) {
				vm = getVmsCreatedList().get(vmIndex);
			} else {
				// Submit the cloudlet to the specific VM.
				vm = VmList.getById(getVmsCreatedList(), cloudlet.getVmId());
				if (vm == null) {
					// VM was not created.
					Log.printLine(CloudSim.clock() + ": " + getName()
							+ ": Postponing execution of cloudlet " + cloudlet.getCloudletId() + " - VM not available");
					continue;
				}
			}
			vmIndex = (vmIndex + 1) % getVmsCreatedList().size();

			Task task = (Task) cloudlet;
			TaskExecutionResourceStatus resourceStatus = canExecuteTaskOnResource(cloudlet, TaskUtils.getTaskFileSize(task.getType()), vm);
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
		}

		// Remove submitted cloudlets from the waiting list.
		for (Cloudlet cloudlet : getCloudletSubmittedList()) {
			getCloudletList().remove(cloudlet);
		}
	}

}
