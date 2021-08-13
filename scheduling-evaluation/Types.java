package scheduling_evaluation;

public class Types {

	public enum SimulationType {
		CLOUD_ONLY,
		EDGE_CLOUD;
	}

	public enum DatacenterType {
		CLOUD_DATACENTER,
		EDGE_DATACENTER;
	}

	public enum ResourceType {
		CLOUD_RESOURCE,
		EDGE_RESOURCE_RASPBERRY_PI,
		EDGE_RESOURCE_MOBILE_PHONE;
	}
	
	public enum TaskType {
		RT1,
		RT2,
		RT3,
		RT4,
		WT1,
		WT2,
		WT3,
		WT4;
	}

	public enum BrokerType {
		CLOUD_ONLY_DEFAULT_BROKER,
		EDGE_CLOUD_DEFAULT_BROKER,
		EDGE_CLOUD_ROUND_ROBIN_BROKER,
		EDGE_CLOUD_SJF_BROKER,
		EDGE_CLOUD_MIN_MIN_BROKER,
		EDGE_CLOUD_MAX_MIN_BROKER;
	}

	public enum TaskExecutionResourceStatus {
		SUCCESS,
		FAILURE_EDGE_DRAINED_BATTERY,
		FAILURE_EDGE_LIMITED_MEMORY,
		FAILURE_UNKNOWN;
	}

	public static String datacenterTypeString(DatacenterType type) {
		switch (type) {
		case CLOUD_DATACENTER: {
			return "Cloud-Datacenter-";
		}
		case EDGE_DATACENTER: {
			return "Edge-Datacenter-";
		}
		default: {
			return "";
		}
		}
	}
	
	public static String brokerTypeString(BrokerType type) {
		switch (type) {
		case CLOUD_ONLY_DEFAULT_BROKER: {
			return "Default-Cloud-Broker-";
		}
		case EDGE_CLOUD_DEFAULT_BROKER: {
			return "Default-EdgeCloud-Broker-";
		}
		case EDGE_CLOUD_ROUND_ROBIN_BROKER: {
			return "RoundRobin-EdgeCloud-Broker-";
		}
		case EDGE_CLOUD_SJF_BROKER: {
			return "SJF-EdgeCloud-Broker-";
		}
		case EDGE_CLOUD_MIN_MIN_BROKER: {
			return "MinMin-EdgeCloud-Broker-";
		}
		case EDGE_CLOUD_MAX_MIN_BROKER: {
			return "MaxMin-EdgeCloud-Broker-";
		}
		}
		return "";
	}

}
