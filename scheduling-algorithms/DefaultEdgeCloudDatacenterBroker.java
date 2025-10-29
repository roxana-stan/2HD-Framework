package scheduling_algorithms;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;
import org.edge.core.edge.EdgeDevice;

import scheduling_evaluation.Constants;
import scheduling_evaluation.SimulationUtils;
import scheduling_evaluation.Task;
import scheduling_evaluation.TaskUtils;
import scheduling_evaluation.Types.ResourceType;
import scheduling_evaluation.Types.TaskExecutionResourceStatus;

public class DefaultEdgeCloudDatacenterBroker extends DatacenterBroker {

	public DefaultEdgeCloudDatacenterBroker(String name) throws Exception {
		super(name);
	}

	/**
	 * Process a cloudlet return event.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != $null
	 * @post $none
	 */
	@Override
	protected void processCloudletReturn(SimEvent ev) {
		Cloudlet cloudlet = (Cloudlet) ev.getData();
		getCloudletReceivedList().add(cloudlet);
		Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloudlet " + cloudlet.getCloudletId() + " received");
		cloudletsSubmitted--;
		if (getCloudletList().size() == 0 && cloudletsSubmitted == 0) {
			// All cloudlets executed.
			Log.printLine(CloudSim.clock() + ": " + getName() + ": All cloudlets executed. Finishing...");
			clearDatacenters();
			finishExecution();
		} else {
			// Some cloudlets haven't finished yet.
			if (getCloudletList().size() > 0 && cloudletsSubmitted == 0) {
				// All cloudlets sent finished. Some bound cloudlet is waiting for its VM to be created.
				clearDatacenters();
				/*
				// Do not try to recreate VMs.
				createVmsInDatacenter(0);
				*/
			}
		}
	}

	/**
	 * Create the virtual machines in a datacenter.
	 * 
	 * @param datacenterId Id of the chosen PowerDatacenter
	 * @pre $none
	 * @post $none
	 */
	@Override
	protected void createVmsInDatacenter(int datacenterId) {
		// Send as many VMs as possible for this datacenter before trying the next one.
		int requestedVms = 0;
		String datacenterName = CloudSim.getEntityName(datacenterId);
		Datacenter datacenter = (Datacenter) CloudSim.getEntity(datacenterId);
		for (Vm vm : getVmList()) {
			if (!getVmsToDatacentersMap().containsKey(vm.getId())) {
				// Do not allocate a resource to a datacenter which doesn't support that type of resource.
				if (SimulationUtils.getSupportedResourceType(datacenter) != SimulationUtils.getResourceType(vm)) {
					Log.printLine(CloudSim.clock() + ": " + getName()
						+ ": Skip creation of VM #" + vm.getId() + " in " + datacenterName + " - Resource type mismatch!");
					continue;
				}

				Log.printLine(CloudSim.clock() + ": " + getName()
						+ ": Trying to create VM #" + vm.getId() + " in " + datacenterName);
				sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
				requestedVms++;
			}
		}

		getDatacenterRequestedIdsList().add(datacenterId);

		setVmsRequested(requestedVms);
		setVmsAcks(0);
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

			// Debugging.
			TaskUtils.printExecutionTimes(cloudlet, vm);

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

	protected double getEdgeDeviceBatteryConsumption(Cloudlet cloudlet, Vm vm) {
		ResourceType resourceType = SimulationUtils.getResourceType(vm);
		if (resourceType == ResourceType.CLOUD_RESOURCE) {
			// Cloud resource, not battery powered.
			return 0.0;
		}

		double processingTime = TaskUtils.getProcessingTime(cloudlet, vm);
		double transferTime = TaskUtils.getTransferTime(cloudlet, vm);

		double consumption = processingTime * 1.0;
		if (resourceType == ResourceType.EDGE_RESOURCE_MOBILE_PHONE) {
			consumption += transferTime * Constants.SMARTPHONE_PM_BATTERY_DRAINAGE_RATE;
		} else if (resourceType == ResourceType.EDGE_RESOURCE_RASPBERRY_PI) {
			consumption += transferTime * Constants.RASPBERRY_PI_PM_BATTERY_DRAINAGE_RATE;
		}

		return consumption;
	}

	protected void updateEdgeDeviceBattery(Cloudlet cloudlet, Vm vm) {
		ResourceType resourceType = SimulationUtils.getResourceType(vm);
		if (resourceType == ResourceType.CLOUD_RESOURCE) {
			// Cloud resource, not battery powered.
			return;
		}

		EdgeDevice edgeDevice = (EdgeDevice) vm.getHost();
		if (!edgeDevice.isEnabled()) {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": Battery of EdgeDevice #" + edgeDevice.getId() + " is already drained!" );
			return;
		}

		double batteryConsumption = getEdgeDeviceBatteryConsumption(cloudlet, vm);
		if (batteryConsumption != Constants.INVALID_RESULT_DOUBLE) {
			edgeDevice.updateBatteryCurrentCapacity(batteryConsumption);
		}
	}

	protected TaskExecutionResourceStatus canExecuteTaskOnResource(Cloudlet cloudlet, double cloudletDataSize, Vm vm) {
		ResourceType resourceType = SimulationUtils.getResourceType(vm);

		// A cloud resource can always execute the task.
		if (resourceType == ResourceType.CLOUD_RESOURCE) {
			return TaskExecutionResourceStatus.SUCCESS;
		}

		// Check if the edge resource has enough memory to execute the task.
		if (!TaskUtils.canExecuteTaskOnResourceWithLimitedMemoryCapacity(cloudletDataSize, resourceType)) {
			return TaskExecutionResourceStatus.FAILURE_EDGE_LIMITED_MEMORY;
		}

		// Check if the edge device has enough battery to execute the task.
		EdgeDevice edgeDevice = (EdgeDevice) vm.getHost();
		if (!edgeDevice.isEnabled()) {
			return TaskExecutionResourceStatus.FAILURE_EDGE_DRAINED_BATTERY;
		}

		double batteryBefore = edgeDevice.getCurrentBatteryCapacity();
		double batteryConsumption = getEdgeDeviceBatteryConsumption(cloudlet, vm);
		if (batteryConsumption == Constants.INVALID_RESULT_DOUBLE) {
			return TaskExecutionResourceStatus.FAILURE_UNKNOWN;
		}
		double batteryAfter = batteryBefore - batteryConsumption;
		// Have at least 20% remaining battery after processing.
		boolean lowBattery = (batteryAfter < (0.20 * edgeDevice.getMaxBatteryCapacity()));
		return (lowBattery ? TaskExecutionResourceStatus.FAILURE_EDGE_DRAINED_BATTERY : TaskExecutionResourceStatus.SUCCESS);
	}

}
