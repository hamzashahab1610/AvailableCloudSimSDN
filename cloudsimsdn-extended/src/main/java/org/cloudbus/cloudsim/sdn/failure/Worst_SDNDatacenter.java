package org.cloudbus.cloudsim.sdn.failure;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
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
import org.cloudbus.cloudsim.sdn.workload.Transmission;
import org.cloudbus.cloudsim.sdn.Packet;

import java.util.*;

public class Worst_SDNDatacenter extends SDNDatacenter {
  private static final int FAILURE_EVENT = 9999;
  private static final int RECOVERY_EVENT = 9998;
  private static final int VM_FAILURE_EVENT = 9997;
  private static final int VM_RECOVERY_EVENT = 9996;

  public enum VmStatus {
    ACTIVE,
    FAILED
  }

  private static final double VM_RECOVERY_DELAY = 0.2;

  private FailureGenerator failureGenerator;
  private Map<String, Integer> hostNameToId = new HashMap<>();
  private List<FailureGenerator.Event> failureEvents = new ArrayList<>();
  private Map<String, VmStatus> vmStatus = new HashMap<>();
  private VmAllocationPolicyFromFile vmAllocation;
  private boolean isSplitJoin = false;
  private Map<Integer, List<SDNVm>> failedVmsPerHost = new HashMap<>();
  private Map<String, Integer> vmToOriginalHostId = new HashMap<>();
  private Map<String, List<Cloudlet>> vmCloudletMap = new HashMap<>();

  private double totalServiceDowntime = 0.0;
  private int totalFailures = 0;
  private double totalCloudletsFailed = 0;
  private double totalProcessingDelay = 0.0;
  private double totalTransmissionDelay = 0.0;
  private Map<String, Double> vmFailureTimes = new HashMap<>();

  public Worst_SDNDatacenter(String name, DatacenterCharacteristics characteristics,
      VmAllocationPolicy vmAllocationPolicy, List<Storage> storageList,
      double schedulingInterval, NetworkOperatingSystem nos) throws Exception {
    super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval, nos);

    this.vmAllocation = (VmAllocationPolicyFromFile) vmAllocationPolicy;
    placementsFile = vmAllocation.getPlacementsFile();
    this.isSplitJoin = super.isSplitJoin() == "split-join" ? true : false;
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

    for (Vm vm : getVmList()) {
      SDNVm sdnVm = (SDNVm) vm;
      vmStatus.put(sdnVm.getName(), VmStatus.ACTIVE);
      vmToOriginalHostId.put(sdnVm.getName(), vm.getHost().getId());
    }

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
      case VM_FAILURE_EVENT:
        processVmDestroy(ev, false);
        break;
      case VM_RECOVERY_EVENT:
        processVmRecovery((Map<String, Object>) ev.getData());
        break;
      default:
        super.processEvent(ev);
    }
  }

  protected void processHostFailure(int hostId) {
    Host host = HostList.getById(getHostList(), hostId);

    if (host == null)
      return;

    Log.printLine(CloudSim.clock() + ": Host " + hostId + " has failed");
    totalFailures++;

    List<Vm> vmsToHandle = new ArrayList<>(host.getVmList());

    for (Vm vm : vmsToHandle) {
      SDNVm sdnVm = (SDNVm) vm;

      failedVmsPerHost.computeIfAbsent(vm.getHost().getId(), k -> new ArrayList<>()).add(sdnVm);

      sendNow(getId(), CloudSimTags.VM_DESTROY, sdnVm);
    }

    host.setFailed(true);
  }

  @Override
  protected void processVmDestroy(SimEvent ev, boolean ack) {
    SDNVm vm = (SDNVm) ev.getData();
    vmFailureTimes.put(vm.getName(), CloudSim.clock());

    if (vmStatus.get(vm.getName()) == VmStatus.FAILED) {
      Log.printLine(CloudSim.clock() + ": Warning - VM " + vm.getName() + " is already failed.");
      return;
    }

    vmStatus.put(vm.getName(), VmStatus.FAILED);

    vmAllocation.deallocateHostForVm(vm);
  }

  private void processHostRecovery(int hostId) {
    Host host = HostList.getById(getHostList(), hostId);

    if (host == null)
      return;

    Log.printLine(CloudSim.clock() + ": Host " + hostId + " has recovered");

    host.setFailed(false);

    List<SDNVm> vmsToRecover = failedVmsPerHost.getOrDefault(hostId, new ArrayList<>());

    if (vmsToRecover.isEmpty()) {
      Log.printLine(CloudSim.clock() + ": No VMs to recover on host " + hostId);
      return;
    }

    Log.printLine(CloudSim.clock() + ": Recovering " + vmsToRecover.size() + " VMs on host " + hostId);

    for (SDNVm vm : vmsToRecover) {
      Map<String, Object> recoveryData = new HashMap<>();
      recoveryData.put("vm", vm);
      recoveryData.put("hostId", hostId);

      send(getId(), VM_RECOVERY_DELAY, VM_RECOVERY_EVENT, recoveryData);
    }

    failedVmsPerHost.put(hostId, new ArrayList<>());
  }

  private void processVmRecovery(Map<String, Object> data) {
    SDNVm vm = (SDNVm) data.get("vm");
    int hostId = (Integer) data.get("hostId");

    Host host = HostList.getById(getHostList(), hostId);

    String vmName = vm.getName();
    double failureTime = vmFailureTimes.getOrDefault(vmName, 0.0);
    double downtime = CloudSim.clock() - failureTime;

    totalServiceDowntime += downtime;

    Log.printLine(CloudSim.clock() + ": Service downtime for VM " + vmName +
        " was " + String.format("%.3f", downtime) + " seconds");

    boolean result = getVmAllocationPolicy().allocateHostForVm(vm, host);

    if (result) {
      Log.printLine(CloudSim.clock() + ": Successfully recovered VM " + vmName + " on host " + hostId);

      vmStatus.put(vmName, VmStatus.ACTIVE);

      if (vm.isBeingInstantiated()) {
        vm.setBeingInstantiated(false);
      }

      vm.updateVmProcessing(CloudSim.clock(),
          getVmAllocationPolicy().getHost(vm).getVmScheduler().getAllocatedMipsForVm(vm));

      List<Cloudlet> cancelledCloudlets = vmCloudletMap.getOrDefault(vmName, new ArrayList<>());

      for (Cloudlet cloudlet : cancelledCloudlets) {
        totalProcessingDelay += downtime;
        vm.getCloudletScheduler().cloudletSubmit(cloudlet);
      }

      vmCloudletMap.remove(vmName);
      vmFailureTimes.remove(vmName);
    } else {
      Log.printLine(CloudSim.clock() + ": Failed to recover VM " + vmName + " on host " + hostId);
    }
  }

  @Override
  protected void processNextActivityTransmission(Transmission tr) {
    Packet pkt = tr.getPacket();
    int flowId = pkt.getFlowId();

    double requestedBandwidth = getNOS().getRequestedBandwidth(flowId);
    double transmissionDelay = 0.0;

    if (requestedBandwidth > 0) {
      transmissionDelay = (double) pkt.getSize() * 8 / requestedBandwidth;
      totalTransmissionDelay += transmissionDelay;
    }

    super.processNextActivityTransmission(tr);
  }

  @Override
  protected void processNextActivityProcessing(Processing proc, Request reqAfterCloudlet) {
    Cloudlet cl = proc.getCloudlet();
    int vmId = cl.getVmId();
    SDNVm vm = (SDNVm) VmList.getById(getVmList(), vmId);

    proc.clearCloudlet();

    totalProcessingDelay += cl.getCloudletLength() / (vm.getMips() * vm.getNumberOfPes());

    requestsTable.put(cl.getCloudletId(), reqAfterCloudlet);

    if (vm == null || vm.getHost() == null ||
        vm.getHost().isFailed() || vmStatus.getOrDefault(vm.getName(), VmStatus.ACTIVE) == VmStatus.FAILED) {

      totalCloudletsFailed++;

      if (vm != null) {
        Cloudlet clonedCloudlet = new Cloudlet(cl.getCloudletId(), cl.getCloudletLength(),
            cl.getNumberOfPes(), cl.getCloudletFileSize(),
            cl.getCloudletOutputSize(), cl.getUtilizationModelCpu(),
            cl.getUtilizationModelRam(), cl.getUtilizationModelBw());

        clonedCloudlet.setVmId(cl.getVmId());
        clonedCloudlet.setUserId(cl.getUserId());

        totalProcessingDelay += CloudSim.clock() - cl.getSubmissionTime();

        vm.getCloudletScheduler().cloudletCancel(cl.getCloudletId());

        vmCloudletMap.computeIfAbsent(vm.getName(), k -> new ArrayList<>()).add(clonedCloudlet);
      }

      return;
    }

    sendNow(getId(), CloudSimTags.CLOUDLET_SUBMIT, cl);

    int userId = cl.getUserId();

    Host host = getVmAllocationPolicy().getHost(vmId, userId);
    if (host == null) {
      Vm orgVm = getNOS().getSFForwarderOriginalVm(vmId);
      if (orgVm != null) {
        vmId = orgVm.getId();
        cl.setVmId(vmId);
        host = getVmAllocationPolicy().getHost(vmId, userId);
      } else {
        Log.printLine(CloudSim.clock() + ": Error! cannot find a host for Workload:" + proc + ". VM=" + vmId);
        return;
      }
    }

    vm = (SDNVm) host.getVm(vmId, userId);
    double mips = vm.getMips();
    proc.setVmMipsPerPE(mips);
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