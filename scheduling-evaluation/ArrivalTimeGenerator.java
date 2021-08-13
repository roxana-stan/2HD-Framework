package scheduling_evaluation;

public class ArrivalTimeGenerator {

	public static void main(String[] args) {
		int taskCount = Constants.TASK_COUNT;

		SimulationUtils.generateTaskArrivalTimes(taskCount);

		int brokerId = 1;
		int taskFirstId = 1;
		double[] taskArrivalTimes = SimulationUtils.loadTaskArrivalTimes(taskCount);
		EntityCreator.createTasks(brokerId, taskFirstId, taskArrivalTimes);
	}
}
