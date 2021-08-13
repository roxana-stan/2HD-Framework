package scheduling_evaluation;

import org.cloudbus.cloudsim.Log;

import scheduling_evaluation.Types.BrokerType;

public class Simulation {

	public static void main(String[] args) {
		/*
		 * Broker types:
		 * - EDGE_CLOUD_ROUND_ROBIN_BROKER
		 * - EDGE_CLOUD_SJF_BROKER
		 * - EDGE_CLOUD_MIN_MIN_BROKER
		 * - EDGE_CLOUD_MAX_MIN_BROKER
		 */
		BrokerType brokerType = BrokerType.EDGE_CLOUD_MIN_MIN_BROKER;

		boolean enableEdgeCloud = true;
		if (enableEdgeCloud) {
			EdgeSimulation.createEdgeCloudSimulation(brokerType);
		} else {
			CloudSimulation.createCloudOnlySimulation(brokerType);
		}

		Log.printLine(enableEdgeCloud ? "EDGE-CLOUD simulation done" : "CLOUD-only simulation done");
		Log.printLine(brokerType);
	}
}
