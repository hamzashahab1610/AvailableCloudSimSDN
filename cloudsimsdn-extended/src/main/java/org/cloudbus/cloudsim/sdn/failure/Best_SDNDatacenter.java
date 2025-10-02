package org.cloudbus.cloudsim.sdn.failure;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.HostList;
import org.cloudbus.cloudsim.lists.VmList;
import org.cloudbus.cloudsim.sdn.nos.NetworkOperatingSystem;
import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNDatacenter;
import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNHost;
import org.cloudbus.cloudsim.sdn.policies.vmallocation.VmAllocationPolicyFromFile;
import org.cloudbus.cloudsim.sdn.virtualcomponents.SDNVm;
import org.cloudbus.cloudsim.sdn.workload.Processing;
import org.cloudbus.cloudsim.sdn.workload.Request;
import org.cloudbus.cloudsim.sdn.Packet;

import java.util.*;

public class Best_SDNDatacenter extends SDNDatacenter {
  private static final int FAILURE_EVENT = 9999;
  private static final int RECOVERY_EVENT = 9998;

  private VmAllocationPolicyFromFile vmAllocation;
  private FailureGenerator failureGenerator;
  private Map<String, Integer> hostNameToId = new HashMap<>();
  private List<FailureGenerator.Event> failureEvents = new ArrayList<>();

  private double totalServiceDowntime = 0.0;
  private int totalFailures = 0;
  private double totalCloudletsFailed = 0;
  private double totalProcessingDelay = 0.0;
  private double totalTransmissionDelay = 0.0;

  public Best_SDNDatacenter(String name, DatacenterCharacteristics characteristics,
      VmAllocationPolicy vmAllocationPolicy, List<Storage> storageList,
      double schedulingInterval, NetworkOperatingSystem nos) throws Exception {
    super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval, nos);

    this.vmAllocation = (VmAllocationPolicyFromFile) vmAllocationPolicy;
    this.failureGenerator = new FailureGenerator("dataset-availability-sustainability/machine_events.csv",
        vmAllocation,
        "host");
    this.failureEvents = failureGenerator.getEvents();

    for (Host host : getHostList()) {
      SDNHost sdnHost = (SDNHost) host;
      hostNameToId.put(sdnHost.getName(), host.getId());
    }
  }

  @Override
  public void startEntity() {
    super.startEntity();

    if (failureEvents == null || failureEvents.isEmpty())
      return;

    double currentTime = CloudSim.clock();

    for (FailureGenerator.Event event : failureEvents) {
      Integer hostId = hostNameToId.get(event.nodeID);

      if (hostId == null) {
        Log.printLine(CloudSim.clock() + ": Warning - Could not find host ID for machine " + event.nodeID);
        continue;
      }

      double scheduleTime = currentTime + event.timestamp;

      int eventType = (event.eventType == this.failureGenerator.FAILURE_EVENT) ? FAILURE_EVENT : RECOVERY_EVENT;

      send(getId(), scheduleTime, eventType, hostId);

      Log.printLine(String.format("%f: Scheduled %s event for host %s(ID:%d) at %f",
          CloudSim.clock(),
          eventType == FAILURE_EVENT ? "failure" : "recovery",
          event.nodeID, hostId, scheduleTime));
    }
  }

  @Override
  public void processEvent(SimEvent ev) {
    switch (ev.getTag()) {
      case FAILURE_EVENT:
        processHostFailure((Integer) ev.getData());
        break;
      case RECOVERY_EVENT:
        processHostRecovery((Integer) ev.getData());
        break;
      default:
        super.processEvent(ev);
    }
  }

  protected void processHostFailure(int hostId) {
    Host host = HostList.getById(getHostList(), hostId);

    if (host == null)
      return;

    totalFailures = 0;
  }

  private void processHostRecovery(int hostId) {
    Host host = HostList.getById(getHostList(), hostId);

    if (host == null)
      return;

    totalFailures = 0;
  }

  @Override
  protected void processPacketCompleted(Packet pkt) {
    double transmissionDelay = CloudSim.clock() - pkt.getStartTime();
    totalTransmissionDelay += transmissionDelay;

    super.processPacketCompleted(pkt);
  }

  @Override
  protected void processNextActivityProcessing(Processing proc, Request reqAfterCloudlet) {
    Cloudlet cl = proc.getCloudlet();
    int vmId = cl.getVmId();
    SDNVm vm = (SDNVm) VmList.getById(getVmList(), vmId);

    long cloudletLength = cl.getCloudletLength();
    double totalMips = vm.getMips() * vm.getNumberOfPes();

    double processingDelay = cloudletLength / totalMips;
    totalProcessingDelay += processingDelay;

    super.processNextActivityProcessing(proc, reqAfterCloudlet);
  }

  public void printFailureMetrics(double finishTime) {
    double serviceAvailability = ((finishTime - totalServiceDowntime) / finishTime) * 100.0;

    Log.printLine("========== FAILURE METRICS ==========");
    Log.printLine(String.format("Service Availability: %.3f%%", serviceAvailability));
    Log.printLine(String.format("Total Simulation Time: %.3f", finishTime));
    Log.printLine("Total Cloudlets Failed: " + String.format("%.0f", totalCloudletsFailed));
    Log.printLine("Total Processing Delay: " + totalProcessingDelay);
    Log.printLine("Total Transmission Delay: " + totalTransmissionDelay);
    Log.printLine("Total Delay: " + (totalProcessingDelay + totalTransmissionDelay));
    Log.printLine("Total Failures: " + totalFailures);
  }
}