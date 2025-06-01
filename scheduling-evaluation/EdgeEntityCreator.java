package scheduling_evaluation;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.edge.core.edge.EdgeDataCenter;
import org.edge.core.edge.EdgeDatacenterCharacteristics;
import org.edge.core.edge.EdgeDevice;
import org.edge.core.edge.MicroELement;
import org.edge.core.feature.EdgeType;
import org.edge.core.feature.Mobility;
import org.edge.core.feature.Mobility.Location;
import org.edge.core.feature.operation.IdentityOperation;
import org.edge.core.iot.CarSensor;
import org.edge.core.iot.IoTDevice;
import org.edge.core.iot.LightSensor;
import org.edge.core.iot.TemperatureSensor;
import org.edge.core.iot.VoiceSensor;
import org.edge.network.NetworkModel;
import org.edge.network.NetworkType;
import org.edge.protocol.AMQPProtocol;
import org.edge.protocol.CoAPProtocol;
import org.edge.protocol.CommunicationProtocol;
import org.edge.protocol.CommunicationProtocolType;
import org.edge.protocol.MQTTProtocol;
import org.edge.protocol.XMPPProtocol;

import scheduling_evaluation.Types.ResourceType;

public class EdgeEntityCreator {

	/**
	 * Creates a list of edge resources.
	 * @param userId User or broker ID.
	 * @param count Number of MEL edge resources to be created.
	 * @param resId ID of the first resource to be created.
	 * @param type Type of resources to be created.
	 * @return The created edge resource list.
	 */
	public static List<MicroELement> createEdgeResources(int userId, int count, int resId, ResourceType type) {
		LinkedList<MicroELement> melList = new LinkedList<MicroELement>();

		for (int idx = 0; idx < count; idx++) {
			MicroELement mel = new MicroELement(resId++, userId,
												ResourceUtils.getVmMips(type), ResourceUtils.getVmPesNumber(type),
												ResourceUtils.getVmRam(type), ResourceUtils.getVmBandwidth(type),
												ResourceUtils.getVmStorage(type), Constants.VMM, new CloudletSchedulerSpaceShared());

			EdgeType edgeType = null;
			if (type == ResourceType.EDGE_RESOURCE_RASPBERRY_PI) {
				edgeType = EdgeType.RASPBERRY_PI;
			} else if (type == ResourceType.EDGE_RESOURCE_MOBILE_PHONE) {
				edgeType = EdgeType.MOBILE_PHONE;
			}
			mel.setType(edgeType);
			mel.setEdgeOperation(new IdentityOperation());

			melList.add(mel);
		}

		return melList;
	}

	/**
	 * Creates a datacenter of edge type.
	 * @param name Datacenter name.
	 * @param count Number of edge devices to be created in datacenter.
	 * @param type Type of resources the datacenter supports.
	 * @return The created EdgeDataCenter object.
	 */
	public static EdgeDataCenter createEdgeDatacenter(String name, int count, ResourceType type) {
		/* Create a list to store physical machines. */
		List<EdgeDevice> pmList = new ArrayList<EdgeDevice>();

		for (int idx = 0; idx < count; ++idx) {
			// Create Pe list. Each EdgeDevice has 1 Pe.
			List<Pe> peList = new ArrayList<Pe>();
			peList.add(new Pe(0, new PeProvisionerSimple(ResourceUtils.getPmMips(type))));

			// Create NetworkModel.
			NetworkModel networkModel = createNetworkModel(NetworkType.WIFI, CommunicationProtocolType.XMPP);

			// Create Mobility.
			Mobility mobility = new Mobility(new Location(0.0, 0.0, 0.0));
			mobility.movable = false;
			mobility.volecity = 0.0;
			mobility.signalRange = 100.0;

			// Create EdgeDevice.
			EdgeType edgeType = null;
			if (type == ResourceType.EDGE_RESOURCE_RASPBERRY_PI) {
				edgeType = EdgeType.RASPBERRY_PI;
			} else if (type == ResourceType.EDGE_RESOURCE_MOBILE_PHONE) {
				edgeType = EdgeType.MOBILE_PHONE;
			}
			EdgeDevice edgeDevice = new EdgeDevice(idx,
												new RamProvisionerSimple(ResourceUtils.getPmRam(type)),
												new BwProvisionerSimple(ResourceUtils.getPmBandwidth(type)),
												ResourceUtils.getPmStorage(type),
												peList, new VmSchedulerTimeShared(peList),
												edgeType,
												networkModel,
												ResourceUtils.getPmIotDeviceCapacity(type),
												ResourceUtils.getPmBatteryMaxCapacity(type),
												ResourceUtils.getPmBatteryDrainageRate(type),
												ResourceUtils.getPmBatteryCurrentCapacity(type));
			edgeDevice.setMobility(mobility);

			pmList.add(edgeDevice);
		}

		/* Create EdgeDatacenterCharacteristics storing properties of an EdgeDataCenter. */
		double timeZone = 10.0;
		double costPerSec = 3.0;
		double costPerMem = 0.05;
		double costPerStorage = 0.1;
		double costPerBw = 0.1;
		EdgeDatacenterCharacteristics characteristics = new EdgeDatacenterCharacteristics(Constants.ARCHITECTURE, Constants.OS, Constants.VMM,
																				pmList, timeZone,
																				costPerSec, costPerMem, costPerStorage, costPerBw,
																				getAllCommunicationProtocolClasses(),
																				getAllIotDeviceClasses());

		/* Create 1 EdgeDataCenter. */
		EdgeDataCenter datacenter = null;
		try {
			LinkedList<Storage> storageList = new LinkedList<Storage>();
			datacenter = new EdgeDataCenter(name, characteristics, new VmAllocationPolicySimple(pmList), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return datacenter;
	}

	private static NetworkModel createNetworkModel(NetworkType networkType, CommunicationProtocolType communicationProtocolType) {
		CommunicationProtocol communicationProtocol = null;
		switch (communicationProtocolType) {
		case XMPP:
			communicationProtocol = new XMPPProtocol();
			break;
		case MQTT:
			communicationProtocol = new MQTTProtocol();
			break;
		case COAP:
			communicationProtocol = new CoAPProtocol();
			break;
		case AMQP:
			communicationProtocol = new AMQPProtocol();
			break;
		}

		NetworkModel networkModel = new NetworkModel(networkType);
		networkModel.setCommunicationProtocol(communicationProtocol);
		return networkModel;
	}

	@SuppressWarnings("unchecked")
	private static Class<? extends CommunicationProtocol>[] getAllCommunicationProtocolClasses() {
		Class<? extends CommunicationProtocol>[] communicationProtocolClasses = new Class[4];
		int idx = 0;
		communicationProtocolClasses[idx++] = XMPPProtocol.class;
		communicationProtocolClasses[idx++] = MQTTProtocol.class;
		communicationProtocolClasses[idx++] = CoAPProtocol.class;
		communicationProtocolClasses[idx++] = AMQPProtocol.class;

		return communicationProtocolClasses;
	}

	@SuppressWarnings("unchecked")
	private static Class<? extends IoTDevice>[] getAllIotDeviceClasses() {
		Class<? extends IoTDevice>[] iotDeviceClasses = new Class[4];
		int idx = 0;
		iotDeviceClasses[idx++] = CarSensor.class;
		iotDeviceClasses[idx++] = LightSensor.class;
		iotDeviceClasses[idx++] = TemperatureSensor.class;
		iotDeviceClasses[idx++] = VoiceSensor.class;

		return iotDeviceClasses;
	}

}
