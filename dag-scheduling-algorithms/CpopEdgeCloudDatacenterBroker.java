package dag_scheduling_algorithms;

import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.lists.VmList;

import scheduling_evaluation.Constants;
import scheduling_evaluation.DagSchedulingMetrics;
import scheduling_evaluation.DagUtils;
import scheduling_evaluation.Pair;
import scheduling_evaluation.Task;
import scheduling_evaluation.TaskGraph;
import scheduling_evaluation.TaskUtils;
import scheduling_evaluation.Types.ResourceType;
import scheduling_evaluation.Types.TaskExecutionResourceStatus;

public class CpopEdgeCloudDatacenterBroker extends DefaultDagEdgeCloudDatacenterBroker {

	public CpopEdgeCloudDatacenterBroker(String name, TaskGraph taskGraph) throws Exception {
		super(name, taskGraph);

		int taskCount = taskGraph.getTaskCount();
		int resourceCount = taskGraph.getResourceCount();

		this.taskToResourceMappings = new HashMap<Integer, Integer>(taskCount);

		this.taskAFT = new HashMap<Integer, Double>(taskCount);

		this.resourceAllocatedTimeSlots = new HashMap<Integer, List<Pair<Double, Double>>>(resourceCount);
		for (Integer resource : this.taskGraph.getResources().keySet()) {
			this.resourceAllocatedTimeSlots.put(resource, new LinkedList<Pair<Double, Double>>());
		}

		this.cpopTasks = new PriorityQueue<>(
				(task1, task2) -> Double.compare(this.taskCpopRankMappings.get(task2), this.taskCpopRankMappings.get(task1)));
	}

	/**
	 * Submit cloudlets to the created VMs.
	 * 
	 * @pre $none
	 * @post $none
	 */
	@Override
	protected void submitCloudlets() {
		Instant startTime = Instant.now();

		this.taskGraph.clearAndPrecomputeCosts();
		computeCpopRanks();
		findCriticalPath();

		// DAG task scheduling.
		computeSchedule();

		DagUtils.setTaskCount(this.taskGraph.getTaskCount());

		for (Cloudlet cloudlet : getCloudletList()) {
			int taskId = cloudlet.getCloudletId();

			// Based on the computed schedule, obtain the assigned resource for the current task.
			Vm vm = VmList.getById(getVmsCreatedList(), this.taskToResourceMappings.get(taskId));
			int vmId = vm.getId();

			Task task = (Task) cloudlet;
			TaskExecutionResourceStatus resourceStatus = canExecuteTaskOnResource(cloudlet, this.taskGraph.getTaskInputData(taskId), vm);
			task.setResourceStatus(resourceStatus);

			if (resourceStatus != TaskExecutionResourceStatus.SUCCESS) {
				System.out.println(CloudSim.clock() + ": " + getName() + ": Cannot send cloudlet " + taskId + " to VM #" + vmId
							+ " - Error: " + resourceStatus);
				continue;
			}

			// Update the current capacity of the edge device's battery.
			updateEdgeDeviceBattery(cloudlet, vm);

			// Update the task's length according to the task's actual processing time on the selected resource.
			Double computationTime = getCloudletComputationTime(taskId, vmId);
			task.setTotalExecutionTime(computationTime);
			cloudlet.setCloudletLength((long) (computationTime * vm.getMips()));

			Log.printLine(CloudSim.clock() + ": " + getName() + ": Sending cloudlet " + taskId + " to VM #" + vmId);
			cloudlet.setVmId(vmId);
			send(getVmsToDatacentersMap().get(vmId), this.taskAFT.get(taskId) - computationTime, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
			cloudletsSubmitted++;
			getCloudletSubmittedList().add(cloudlet);
		}

		// Remove submitted cloudlets from the waiting list.
		for (Cloudlet cloudlet : getCloudletSubmittedList()) {
			getCloudletList().remove(cloudlet);
		}

		Instant endTime = Instant.now();

		Duration schedulingTimeDuration = Duration.between(startTime, endTime);
		DagSchedulingMetrics.setSchedulingTimeDuration(schedulingTimeDuration);
	}

	private void computeSchedule() {
		DecimalFormat dft = new DecimalFormat("##.###");

		List<Integer> criticalPathTasks = this.criticalPath.getKey();
		Integer criticalPathResource = this.criticalPath.getValue();

		this.cpopTasks.addAll(this.taskGraph.getEntryTasks());

		while (!this.cpopTasks.isEmpty()) {
			Integer task = this.cpopTasks.poll();
			double taskDataSize = this.taskGraph.getTaskInputData(task);
			Log.printLine("> Attempt to schedule task " + task);

			Double taskEFT = Double.MAX_VALUE;
			Double taskEST = Constants.INVALID_RESULT_DOUBLE;
			Integer allocatedResource = Constants.INVALID_RESULT_INT;
			int newResourceAllocatedTimeSlotIdx = Constants.INVALID_RESULT_INT;

			if (criticalPathTasks.contains(task)) {
				Double computationCost = this.taskGraph.getComputationCost(task, criticalPathResource);
				Double EST = computeEST(task, criticalPathResource);
				Double EFT = computationCost + EST;

				taskEFT = EFT;
				taskEST = EST;
				allocatedResource = criticalPathResource;
				newResourceAllocatedTimeSlotIdx = this.resourceAllocatedTimeSlotIdx;
			} else {
				for (Map.Entry<Integer, ResourceType> resourceEntry : this.taskGraph.getResources().entrySet()) {
					Integer resource = resourceEntry.getKey();
					ResourceType resourceType = resourceEntry.getValue();

					if (!TaskUtils.canExecuteTaskOnResourceWithLimitedMemoryCapacity(taskDataSize, resourceType)
						|| !canExecuteTaskOnResourceWithLimitedBatteryCapacity(task, resource)) {
						continue;
					}

					Double computationCost = this.taskGraph.getComputationCost(task, resource);
					Double EST = computeEST(task, resource);
					Double EFT = computationCost + EST;
					if (EFT < taskEFT) {
						taskEFT = EFT;
						taskEST = EST;
						allocatedResource = resource;
						newResourceAllocatedTimeSlotIdx = this.resourceAllocatedTimeSlotIdx;
					}
				}
			}

			Double taskPriority = this.taskCpopRankMappings.get(task);
			Log.printLine("Task " + task + " (Priority: " + dft.format(taskPriority) + ")"
						+ " -> " + "Resource #" + allocatedResource + " -> " + "AFT: " + taskEFT);
			this.taskToResourceMappings.put(task, allocatedResource);
			this.taskAFT.put(task, taskEFT);
			Pair<Double, Double> newResourceAllocatedTimeSlot = new Pair<Double, Double>(taskEST, taskEFT);
			this.resourceAllocatedTimeSlots.get(allocatedResource).add(newResourceAllocatedTimeSlotIdx, newResourceAllocatedTimeSlot);

			for (Integer succTask : this.taskGraph.getSuccessorTasksInfo(task).keySet()) {
				if (isReadyTask(succTask)) {
					this.cpopTasks.add(succTask);
				}
			}

			Log.printLine("< Finalized attempt to schedule task " + task);
		}
	}

}
