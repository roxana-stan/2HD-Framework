package scheduling_evaluation;

import org.cloudbus.cloudsim.Vm;
import org.edge.core.edge.MicroELement;

import scheduling_evaluation.Types.ResourceType;

public class ResourceUtils {

	public static boolean isEdgeResource(Vm vm) {
		return vm instanceof MicroELement;
	}

	public static boolean isEdgeResource(ResourceType resourceType) {
		return resourceType == ResourceType.EDGE_RESOURCE_MOBILE_PHONE || resourceType == ResourceType.EDGE_RESOURCE_RASPBERRY_PI;
	}

	public static boolean isCloudResource(Vm vm) {
		return !isEdgeResource(vm);
	}

	public static boolean isCloudResource(ResourceType resourceType) {
		return resourceType == ResourceType.CLOUD_RESOURCE;
	}

	/* Virtual machine resource parameters. */

	public static double getVmMips(ResourceType type) {
		switch(type) {
		case CLOUD_RESOURCE:
			return Constants.CLOUD_VM_MIPS;
		case EDGE_RESOURCE_MOBILE_PHONE:
			return Constants.SMARTPHONE_VM_MIPS;
		case EDGE_RESOURCE_RASPBERRY_PI:
			return Constants.RASPBERRY_PI_VM_MIPS;
		}
		return 0.0;
	}

	public static int getVmPesNumber(ResourceType type) {
		switch(type) {
		case CLOUD_RESOURCE:
			return Constants.CLOUD_VM_PES_NUMBER;
		case EDGE_RESOURCE_MOBILE_PHONE:
			return Constants.SMARTPHONE_VM_PES_NUMBER;
		case EDGE_RESOURCE_RASPBERRY_PI:
			return Constants.RASPBERRY_PI_VM_PES_NUMBER;
		}
		return 0;
	}

	public static int getVmRam(ResourceType type) {
		switch(type) {
		case CLOUD_RESOURCE:
			return Constants.CLOUD_VM_RAM;
		case EDGE_RESOURCE_MOBILE_PHONE:
			return Constants.SMARTPHONE_VM_RAM;
		case EDGE_RESOURCE_RASPBERRY_PI:
			return Constants.RASPBERRY_PI_VM_RAM;
		}
		return 0;
	}

	public static long getVmBandwidth(ResourceType type) {
		switch(type) {
		case CLOUD_RESOURCE:
			return Constants.CLOUD_VM_BANDWIDTH;
		case EDGE_RESOURCE_MOBILE_PHONE:
			return Constants.SMARTPHONE_VM_BANDWIDTH;
		case EDGE_RESOURCE_RASPBERRY_PI:
			return Constants.RASPBERRY_PI_VM_BANDWIDTH;
		}
		return 0;
	}

	public static long getVmStorage(ResourceType type) {
		switch(type) {
		case CLOUD_RESOURCE:
			return Constants.CLOUD_VM_STORAGE;
		case EDGE_RESOURCE_MOBILE_PHONE:
			return Constants.SMARTPHONE_VM_STORAGE;
		case EDGE_RESOURCE_RASPBERRY_PI:
			return Constants.RASPBERRY_PI_VM_STORAGE;
		}
		return 0;
	}

	/* Physical machine parameters */

	public static double getPmMips(ResourceType type) {
		switch(type) {
		case CLOUD_RESOURCE:
			return Constants.CLOUD_PM_MIPS;
		case EDGE_RESOURCE_MOBILE_PHONE:
			return Constants.SMARTPHONE_PM_MIPS;
		case EDGE_RESOURCE_RASPBERRY_PI:
			return Constants.RASPBERRY_PI_PM_MIPS;
		}
		return 0.0;
	}

	public static int getPmPesNumber(ResourceType type) {
		switch(type) {
		case CLOUD_RESOURCE:
			return Constants.CLOUD_PM_PES_NUMBER;
		case EDGE_RESOURCE_MOBILE_PHONE:
			return Constants.SMARTPHONE_PM_PES_NUMBER;
		case EDGE_RESOURCE_RASPBERRY_PI:
			return Constants.RASPBERRY_PI_PM_PES_NUMBER;
		}
		return 0;
	}

	public static int getPmRam(ResourceType type) {
		switch(type) {
		case CLOUD_RESOURCE:
			return Constants.CLOUD_PM_RAM;
		case EDGE_RESOURCE_MOBILE_PHONE:
			return Constants.SMARTPHONE_PM_RAM;
		case EDGE_RESOURCE_RASPBERRY_PI:
			return Constants.RASPBERRY_PI_PM_RAM;
		}
		return 0;
	}

	public static double getPmBandwidth(ResourceType type) {
		switch(type) {
		case CLOUD_RESOURCE:
			return Constants.CLOUD_PM_BANDWIDTH;
		case EDGE_RESOURCE_MOBILE_PHONE:
			return Constants.SMARTPHONE_PM_BANDWIDTH;
		case EDGE_RESOURCE_RASPBERRY_PI:
			return Constants.RASPBERRY_PI_PM_BANDWIDTH;
		}
		return 0;
	}

	public static long getPmStorage(ResourceType type) {
		switch(type) {
		case CLOUD_RESOURCE:
			return Constants.CLOUD_PM_STORAGE;
		case EDGE_RESOURCE_MOBILE_PHONE:
			return Constants.SMARTPHONE_PM_STORAGE;
		case EDGE_RESOURCE_RASPBERRY_PI:
			return Constants.RASPBERRY_PI_PM_STORAGE;
		}
		return 0;
	}

	public static int getPmIotDeviceCapacity(ResourceType type) {
		switch(type) {
		case EDGE_RESOURCE_MOBILE_PHONE:
			return Constants.SMARTPHONE_PM_IOTDEVICE_CAPACITY;
		case EDGE_RESOURCE_RASPBERRY_PI:
			return Constants.RASPBERRY_PI_PM_IOTDEVICE_CAPACITY;
		default:
			return 0;
		}
	}

	public static double getPmBatteryMaxCapacity(ResourceType type) {
		switch(type) {
		case EDGE_RESOURCE_MOBILE_PHONE:
			return Constants.SMARTPHONE_PM_BATTERY_MAX_CAPACITY;
		case EDGE_RESOURCE_RASPBERRY_PI:
			return Constants.RASPBERRY_PI_PM_BATTERY_MAX_CAPACITY;
		default:
			return 0;
		}
	}

	public static double getPmBatteryDrainageRate(ResourceType type) {
		switch(type) {
		case EDGE_RESOURCE_MOBILE_PHONE:
			return Constants.SMARTPHONE_PM_BATTERY_DRAINAGE_RATE;
		case EDGE_RESOURCE_RASPBERRY_PI:
			return Constants.RASPBERRY_PI_PM_BATTERY_DRAINAGE_RATE;
		default:
			return 0;
		}
	}

	public static double getPmBatteryCurrentCapacity(ResourceType type) {
		switch(type) {
		case EDGE_RESOURCE_MOBILE_PHONE:
			return Constants.SMARTPHONE_PM_BATTERY_CURRENT_CAPACITY;
		case EDGE_RESOURCE_RASPBERRY_PI:
			return Constants.RASPBERRY_PI_PM_BATTERY_CURRENT_CAPACITY;
		default:
			return 0;
		}
	}

}
