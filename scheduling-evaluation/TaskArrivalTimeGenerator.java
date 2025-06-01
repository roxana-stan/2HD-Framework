package scheduling_evaluation;

public class TaskArrivalTimeGenerator {

	public static void main(String[] args) {
		int brokerId = 1;
		int taskId = 1;

		int taskCount = Constants.TASK_COUNT;
		SimulationUtils.generateTaskArrivalTimes(taskCount);
		double[] taskArrivalTimes = SimulationUtils.loadTaskArrivalTimes(taskCount);

		EntityCreator.createTasks(brokerId, taskId, taskArrivalTimes);
	}

}
