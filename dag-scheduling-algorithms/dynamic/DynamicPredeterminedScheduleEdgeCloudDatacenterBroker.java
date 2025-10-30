package dag_scheduling_algorithms.dynamic;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.lists.VmList;

import dag_scheduling_algorithms.DefaultDagEdgeCloudDatacenterBroker;
import scheduling_evaluation.ConcurrentUtils;
import scheduling_evaluation.Constants;
import scheduling_evaluation.DagEntityCreator;
import scheduling_evaluation.DagSchedulingMetrics;
import scheduling_evaluation.DagUtils;
import scheduling_evaluation.Pair;
import scheduling_evaluation.Task;
import scheduling_evaluation.TaskGraph;
import scheduling_evaluation.TaskUtils;
import scheduling_evaluation.Types.ResourceType;
import scheduling_evaluation.Types.TaskExecutionResourceStatus;

public class DynamicPredeterminedScheduleEdgeCloudDatacenterBroker extends DefaultDagEdgeCloudDatacenterBroker {

	private ExecutorService executor = null;
	private ReentrantLock lock = null;

	public DynamicPredeterminedScheduleEdgeCloudDatacenterBroker(String name, TaskGraph taskGraph, LinkedList<Integer> schedule) throws Exception {
		super(name, taskGraph);

		int taskCount = taskGraph.getTaskCount();
		int resourceCount = taskGraph.getResourceCount();

		this.taskToResourceMappings = new HashMap<Integer, Integer>(taskCount);

		this.taskAFT = new HashMap<Integer, Double>(taskCount);

		this.resourceAllocatedTimeSlots = new HashMap<Integer, List<Pair<Double, Double>>>(resourceCount);
		for (Integer resource : this.taskGraph.getResources().keySet()) {
			this.resourceAllocatedTimeSlots.put(resource, new LinkedList<Pair<Double, Double>>());
		}

		initializeDynamicTaskSubgraphsInfo();

		this.sortedTasksByPriorityDesc = new LinkedList<Integer>(schedule);

		this.executor = Executors.newFixedThreadPool(1 + this.taskSubgraphCount);
		this.lock = new ReentrantLock();
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

		CountDownLatch latch = new CountDownLatch(1 + this.taskSubgraphCount);
		this.executor.submit(() -> {
			latch.countDown();
		});
		IntStream.range(0, this.taskSubgraphCount).forEach(taskSubgraphIdx -> this.executor.submit(() -> {
			this.lock.lock();
			try {
				Log.printLine("> Attempt to add task subgraph " + taskSubgraphIdx);
				addDynamicTaskSubgraph(taskSubgraphIdx);
			} finally {
				Log.printLine("< Finalized attempt to add task subgraph " + taskSubgraphIdx);
				this.lock.unlock();
				latch.countDown();
			}
		}));
		try {
			latch.await();
		} catch (InterruptedException e) {
			Log.printLine("< Count down latch interrupted; waiting for 60 seconds then killing non-finished tasks");
			e.printStackTrace();
		}
		ConcurrentUtils.stop(this.executor);

		// DAG task scheduling.
		Log.printLine("> " + this.sortedTasksByPriorityDesc.size() + " tasks to be scheduled");
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

	private boolean taskListContainsUnscheduledTasks() {
		this.lock.lock();
		boolean containsUnscheduledTasks = !this.sortedTasksByPriorityDesc.isEmpty();
		this.lock.unlock();
		return containsUnscheduledTasks;
	}

	private void computeSchedule() {
		while (taskListContainsUnscheduledTasks()) {
			this.lock.lock();
			Integer task = Constants.INVALID_RESULT_INT;
			try {
				task = this.sortedTasksByPriorityDesc.getFirst();
				double taskDataSize = this.taskGraph.getTaskInputData(task);
				Log.printLine("> Attempt to schedule task " + task);

				this.sortedTasksByPriorityDesc.remove(task);

				Double taskEFT = Double.MAX_VALUE;
				Double taskEST = Constants.INVALID_RESULT_DOUBLE;
				Integer allocatedResource = Constants.INVALID_RESULT_INT;
				int newResourceAllocatedTimeSlotIdx = Constants.INVALID_RESULT_INT;

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

				Log.printLine("Task " + task
							+ " -> " + "Resource #" + allocatedResource + " -> " + "AFT: " + taskEFT);
				this.taskToResourceMappings.put(task, allocatedResource);
				this.taskAFT.put(task, taskEFT);
				Pair<Double, Double> newResourceAllocatedTimeSlot = new Pair<Double, Double>(taskEST, taskEFT);
				this.resourceAllocatedTimeSlots.get(allocatedResource).add(newResourceAllocatedTimeSlotIdx, newResourceAllocatedTimeSlot);
			} finally {
				Log.printLine("< Finalized attempt to schedule task " + task);
				this.lock.unlock();
			}
		}
	}

	private void addDynamicTaskSubgraph(int taskSubgraphIdx) {
		Double taskSubgraphArrivalTime = this.taskSubgraphArrivalTimes.get(taskSubgraphIdx);
		String taskSubgraphFilename = this.taskSubgraphFilenames.get(taskSubgraphIdx);

		List<Integer> taskIds = DagUtils.loadTaskSubgraph(taskSubgraphFilename, this.taskGraph);

		this.taskGraph.clearAndPrecomputeCosts();

		int brokerId = getId();
		List<? extends Cloudlet> cloudlets = DagEntityCreator.createGenericTasks(brokerId, taskIds, taskSubgraphArrivalTime);
		submitCloudletList(cloudlets);
	}

}
