package scheduling_algorithms;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.lists.VmList;

import scheduling_evaluation.Task;
import scheduling_evaluation.TaskUtils;
import scheduling_evaluation.Types.TaskExecutionResourceStatus;

public class DefaultCloudDatacenterBroker extends DatacenterBroker {

	public DefaultCloudDatacenterBroker(String name) throws Exception {
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
			TaskExecutionResourceStatus resourceStatus = TaskExecutionResourceStatus.SUCCESS;
			task.setResourceStatus(resourceStatus);

			// Update the task's length such that the processing time on the selected resource includes the data transfer time.
			task.setTotalExecutionTime(TaskUtils.getTotalExecutionTime(cloudlet, vm));
			TaskUtils.setCloudletTotalLength(cloudlet, vm);

			Log.printLine(CloudSim.clock() + ": " + getName() + ": Sending cloudlet " + cloudlet.getCloudletId() + " to VM #" + vm.getId());
			cloudlet.setVmId(vm.getId());
			send(getVmsToDatacentersMap().get(vm.getId()), task.getArrivalTime(), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
			cloudletsSubmitted++;
			getCloudletSubmittedList().add(cloudlet);
		}

		// Remove the submitted cloudlets from waiting list.
		for (Cloudlet cloudlet : getCloudletSubmittedList()) {
			getCloudletList().remove(cloudlet);
		}
	}

	/**
	 * Destroy the virtual machines running in datacenters.
	 * 
	 * @pre $none
	 * @post $none
	 */
	@Override
	protected void clearDatacenters() {
		/*
		// Do not destroy the VM instances.
		for (Vm vm : getVmsCreatedList()) {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": Destroying VM #" + vm.getId());
			sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.VM_DESTROY, vm);
		}
		*/

		getVmsCreatedList().clear();
	}

}
