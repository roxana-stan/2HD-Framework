package scheduling_evaluation;

import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.Vm;
import org.edge.core.edge.MicroELement;

import scheduling_evaluation.Types.TaskExecutionResourceStatus;
import scheduling_evaluation.Types.TaskType;

public class Task extends Cloudlet {

	private double arrivalTime = 0.0;
	private TaskType type;
	private TaskExecutionResourceStatus resourceStatus;
	private double totalExecutionTime = -1.0;

	public Task(int cloudletId, double cloudletLength, int pesNumber, double cloudletFileSize, double cloudletOutputSize,
			UtilizationModel utilizationModelCpu, UtilizationModel utilizationModelRam, UtilizationModel utilizationModelBw) {
		super(cloudletId, cloudletLength, pesNumber, cloudletFileSize, cloudletOutputSize,
				utilizationModelCpu, utilizationModelRam, utilizationModelBw);
	}

	public Task(int cloudletId, double cloudletLength, int pesNumber, double cloudletFileSize, double cloudletOutputSize,
			UtilizationModel utilizationModelCpu, UtilizationModel utilizationModelRam, UtilizationModel utilizationModelBw,
			boolean record) {
		super(cloudletId, cloudletLength, pesNumber, cloudletFileSize, cloudletOutputSize,
				utilizationModelCpu, utilizationModelRam, utilizationModelBw, record);
	}

	public Task(int cloudletId, double cloudletLength, int pesNumber, double cloudletFileSize, double cloudletOutputSize,
			UtilizationModel utilizationModelCpu, UtilizationModel utilizationModelRam, UtilizationModel utilizationModelBw,
			boolean record, List<String> fileList) {
		super(cloudletId, cloudletLength, pesNumber, cloudletFileSize, cloudletOutputSize,
				utilizationModelCpu, utilizationModelRam, utilizationModelBw, record, fileList);
	}

	public Task(int cloudletId, double cloudletLength, int pesNumber, double cloudletFileSize, double cloudletOutputSize,
			UtilizationModel utilizationModelCpu, UtilizationModel utilizationModelRam, UtilizationModel utilizationModelBw,
			List<String> fileList) {
		super(cloudletId, cloudletLength, pesNumber, cloudletFileSize, cloudletOutputSize,
				utilizationModelCpu, utilizationModelRam, utilizationModelBw, fileList);
	}

	public double getArrivalTime() {
		return arrivalTime;
	}

	public void setArrivalTime(double arrivalTime) {
		this.arrivalTime = arrivalTime;
	}

	public TaskType getType() {
		return type;
	}

	public void setType(TaskType type) {
		this.type = type;
	}

	public TaskExecutionResourceStatus getResourceStatus() {
		return resourceStatus;
	}

	public void setResourceStatus(TaskExecutionResourceStatus resourceStatus) {
		this.resourceStatus = resourceStatus;
	}

	public double getTotalExecutionTime() {
		return totalExecutionTime;
	}

	public void setTotalExecutionTime(double totalExecutionTime) {
		this.totalExecutionTime = totalExecutionTime;
	}

	public double getWeightedAverageExecutionTime(List<Vm> vms) {
		double totalExecutionTime = 0.0;
		int vmCount = 0;

		for (Vm vm : vms) {
			if ((type == TaskType.RT1 || type == TaskType.RT3) && (vm instanceof MicroELement)) {
				// Cannot process RT1 and RT3 tasks on edge resources
				continue;
			}
			totalExecutionTime += TaskUtils.getTotalExecutionTime(this, vm);
			vmCount++;
		}

		double weightedAverageExecutionTime = totalExecutionTime / vmCount;
		return weightedAverageExecutionTime;
	}

}
