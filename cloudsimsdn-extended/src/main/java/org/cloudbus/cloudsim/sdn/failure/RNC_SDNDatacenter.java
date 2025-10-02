package org.cloudbus.cloudsim.sdn.failure;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.HostList;
import org.cloudbus.cloudsim.lists.VmList;
import org.cloudbus.cloudsim.sdn.CloudSimTagsSDN;
import org.cloudbus.cloudsim.sdn.nos.NetworkOperatingSystem;
import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNDatacenter;
import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNHost;
import org.cloudbus.cloudsim.sdn.policies.vmallocation.VmAllocationPolicyFromFile;
import org.cloudbus.cloudsim.sdn.virtualcomponents.SDNVm;
import org.cloudbus.cloudsim.sdn.workload.Processing;
import org.cloudbus.cloudsim.sdn.workload.Request;
import org.cloudbus.cloudsim.sdn.workload.Transmission;
import org.cloudbus.cloudsim.sdn.Packet;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RNC_SDNDatacenter extends SDNDatacenter {
  private static final int FAILURE_EVENT = 9999;
  private static final int RECOVERY_EVENT = 9998;
  private static final int VM_FAILURE_EVENT = 9997;

  public enum VmStatus {
    ACTIVE,
    INACTIVE,
    FAILED
  }

  private static final double VM_RECOVERY_DELAY = 1.43;

  private FailureGenerator failureGenerator;
  private Map<String, Integer> hostNameToId = new HashMap<>();
  private List<FailureGenerator.Event> failureEvents = new ArrayList<>();
  private Map<String, VmStatus> vmStatus = new HashMap<>();
  private Map<String, List<String>> vmBackups = new HashMap<>();
  private VmAllocationPolicyFromFile vmAllocation;
  private NetworkOperatingSystem nos = getNOS();
  private Map<String, String> activeBackupVmMap = new HashMap<>();

  private double totalServiceDowntime = 0.0;
  private int totalFailures = 0;
  private double totalCloudletsFailed = 0;
  private double totalProcessingDelay = 0.0;
  private double totalTransmissionDelay = 0.0;
  private Map<String, Double> vmFailureTimes = new HashMap<>();

  public RNC_SDNDatacenter(String name, DatacenterCharacteristics characteristics,
      VmAllocationPolicy vmAllocationPolicy, List<Storage> storageList,
      double schedulingInterval, NetworkOperatingSystem nos) throws Exception {
    super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval, nos);

    this.vmAllocation = (VmAllocationPolicyFromFile) vmAllocationPolicy;
    placementsFile = vmAllocation.getPlacementsFile();
    this.failureGenerator = new FailureGenerator("dataset-availability-sustainability/machine_events.csv",
        vmAllocation,
        "host");
    this.failureEvents = failureGenerator.getEvents();

    for (Host host : getHostList()) {
      SDNHost sdnHost = (SDNHost) host;
      hostNameToId.put(sdnHost.getName(), host.getId());
    }

    loadBackupMappings();
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
      case VM_FAILURE_EVENT:
        processVmDestroy(ev, false);
        break;
      default:
        super.processEvent(ev);
    }
  }

  private void processHostRecovery(int hostId) {
    Host host = HostList.getById(getHostList(), hostId);

    if (host != null) {
      Log.printLine(CloudSim.clock() + ": Host " + hostId + " has recovered");
      host.setFailed(false);
    }
  }

  protected void processHostFailure(int hostId) {
    Host host = HostList.getById(getHostList(), hostId);

    if (host == null) {
      return;
    }

    Log.printLine(CloudSim.clock() + ": Host " + hostId + " has failed");

    totalFailures++;

    List<Vm> vmsToHandle = new ArrayList<>(host.getVmList());

    for (Vm vm : vmsToHandle) {
      SDNVm sdnVm = (SDNVm) vm;
      String vmName = sdnVm.getName();

      if (!vmName.contains("-backup-")) {
        sendNow(getId(), CloudSimTags.VM_DESTROY, sdnVm);
      }
    }

    host.setFailed(true);
  }

  @Override
  protected void processVmDestroy(SimEvent ev, boolean ack) {
    SDNVm sourceVm = (SDNVm) ev.getData();
    vmFailureTimes.put(sourceVm.getName(), CloudSim.clock());

    if (vmStatus.get(sourceVm.getName()) == VmStatus.FAILED) {
      Log.printLine(CloudSim.clock() + ": Warning - VM " + sourceVm.getName() + " is already failed.");
      return;
    }

    String originalVmName = sourceVm.getName().contains("-backup-")
        ? sourceVm.getName().substring(0, sourceVm.getName().indexOf("-backup-"))
        : sourceVm.getName();

    vmStatus.put(sourceVm.getName(), VmStatus.FAILED);

    String activatedBackupName = activateBackupVM(originalVmName, sourceVm.getName());

    if (activatedBackupName != null) {
      SDNVm backupVm = findVmByName(getVmList(), activatedBackupName);

      if (backupVm != null) {
        double failureTime = vmFailureTimes.getOrDefault(sourceVm.getName(), 0.0);

        if (failureTime > 0) {
          double downtime = CloudSim.clock() - failureTime;
          totalServiceDowntime += downtime + VM_RECOVERY_DELAY;
          totalProcessingDelay += downtime + VM_RECOVERY_DELAY;

          Log.printLine(CloudSim.clock() + ": Service downtime for VM " + sourceVm.getName() +
              " was " + String.format("%.3f", downtime) + " seconds");
        }

        nos.addExtraPath(sourceVm.getId(), backupVm.getId());
        createNewBackupVM(originalVmName);
      }
    } else {
      Log.printLine(CloudSim.clock() + ": Warning - Failed to activate backup VM for " + sourceVm.getName());

      totalCloudletsFailed += sourceVm.getCloudletScheduler().getCloudletExecList().size() +
          sourceVm.getCloudletScheduler().getCloudletWaitingList().size();
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

    if (vm == null) {
      return;
    }

    totalProcessingDelay += cl.getCloudletLength() / (vm.getMips() * vm.getNumberOfPes());

    requestsTable.put(cl.getCloudletId(), reqAfterCloudlet);

    SDNVm backupVm = findVmByName(getVmList(),
        activeBackupVmMap.get(vm.getName()));

    if (vm == null || vm.getHost() == null ||
        vm.getHost().isFailed() || vmStatus.getOrDefault(vm.getName(), VmStatus.ACTIVE) == VmStatus.FAILED) {

      totalCloudletsFailed++;

      if (vm != null) {
        Cloudlet clonedCloudlet = new Cloudlet(cl.getCloudletId(), cl.getCloudletLength(),
            cl.getNumberOfPes(), cl.getCloudletFileSize(),
            cl.getCloudletOutputSize(), cl.getUtilizationModelCpu(),
            cl.getUtilizationModelRam(), cl.getUtilizationModelBw());

        clonedCloudlet.setVmId(backupVm.getId());
        clonedCloudlet.setUserId(cl.getUserId());

        totalProcessingDelay += CloudSim.clock() - cl.getSubmissionTime();

        vm.getCloudletScheduler().cloudletCancel(cl.getCloudletId());
        backupVm.getCloudletScheduler().cloudletSubmit(clonedCloudlet);
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

  private String activateBackupVM(String originalVmName, String failedVmName) {
    List<String> backupVms = vmBackups.get(originalVmName);

    if (backupVms == null || backupVms.isEmpty()) {
      Log.printLine(CloudSim.clock() + ": Warning - No inactive backup VM available for failed VM " + failedVmName);
      return null;
    }

    for (String backupVmName : backupVms) {
      if (backupVmName.equals(failedVmName)) {
        continue;
      }

      VmStatus status = vmStatus.get(backupVmName);
      SDNVm backupVm = findVmByName(getVmList(), backupVmName);

      if (status == VmStatus.FAILED) {
        continue;
      }

      if (backupVm == null || backupVm.getHost().isFailed()) {
        continue;
      }

      vmStatus.put(backupVmName, VmStatus.ACTIVE);
      activeBackupVmMap.put(failedVmName, backupVmName);

      Log.printLine(CloudSim.clock() + ": Activated backup VM " +
          backupVmName + " for failed VM " + failedVmName);

      return backupVmName;
    }

    return null;
  }

  private void createNewBackupVM(String originalVmName) {
    try {
      ProcessBuilder pb = new ProcessBuilder("python",
          "E:\\OneDrive - University of Regina\\University of Regina\\Research\\Step 3 - Problem Validation\\optimal-sfc-placement\\generate_backup_placement.py",
          originalVmName);

      pb.redirectErrorStream(true);

      Process process = pb.start();

      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      StringBuilder output = new StringBuilder();
      String line;

      while ((line = reader.readLine()) != null) {
        output.append(line);
      }

      int exitCode = process.waitFor();

      if (exitCode != 0) {
        Log.printLine(CloudSim.clock() + ": Python script failed with exit code " + exitCode);
        Log.printLine(CloudSim.clock() + ": Output: " + output.toString());
        return;
      }

      String trimmedOutput = output.toString().trim();

      if (trimmedOutput.isEmpty()) {
        Log.printLine(CloudSim.clock() + ": Error - No output from Python script");
        return;
      }

      String[] parts = trimmedOutput.split(",");

      if (parts.length != 2) {
        Log.printLine(CloudSim.clock() + ": Error - Invalid output format from Python script: " + trimmedOutput);
        return;
      }

      String newBackupVmName = parts[0];
      String targetHostName = parts[1];

      Integer hostId = hostNameToId.get(targetHostName);
      if (hostId == null) {
        Log.printLine(CloudSim.clock() + ": Error - Invalid host name from Python script: " + targetHostName);
        return;
      }

      Host targetHost = HostList.getById(getHostList(), hostId);
      if (targetHost == null || targetHost.isFailed()) {
        Log.printLine(CloudSim.clock() + ": Error - Target host is invalid or failed: " + targetHostName);
        return;
      }

      SDNVm originalVm = findVmByName(getVmList(), originalVmName);
      if (originalVm == null) {
        Log.printLine(CloudSim.clock() + ": Error - Cannot find original VM: " + originalVmName);
        return;
      }

      int vmId = SDNVm.getUniqueVmId();

      SDNVm newBackupVm = new SDNVm(vmId,
          originalVm.getUserId(),
          originalVm.getMips(),
          originalVm.getNumberOfPes(),
          originalVm.getRam(),
          originalVm.getBw(),
          originalVm.getSize(),
          originalVm.getVmm(),
          originalVm.getCloudletScheduler());

      newBackupVm.setName(newBackupVmName);

      Object[] data = new Object[3];
      data[0] = newBackupVm;
      data[1] = this.nos;
      data[2] = targetHost;

      sendNow(getId(), CloudSimTagsSDN.SDN_VM_CREATE_DYNAMIC, data);

      Log.printLine(CloudSim.clock() + ": Requested creation of new backup VM " +
          newBackupVmName + " for " + originalVmName + " on host " + targetHostName);

      vmBackups.computeIfAbsent(originalVmName, k -> new ArrayList<>()).add(newBackupVmName);
      vmStatus.put(newBackupVmName, VmStatus.INACTIVE);

    } catch (Exception e) {
      Log.printLine(CloudSim.clock() + ": Error creating new backup VM: " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Override
  protected boolean processVmCreateDynamic(SimEvent ev) {
    Object[] data = (Object[]) ev.getData();
    SDNVm vm = (SDNVm) data[0];
    NetworkOperatingSystem callbackNOS = (NetworkOperatingSystem) data[1];
    Host targetHost = (Host) data[2];

    boolean result = processVmCreateEvent(vm, targetHost, false);

    data[0] = vm;
    data[1] = result;

    sendNow(callbackNOS.getId(), CloudSimTagsSDN.SDN_VM_CREATE_DYNAMIC_ACK,
        data);

    return result;
  }

  protected boolean processVmCreateEvent(SDNVm vm, Host targetHost, boolean ack) {
    boolean result = getVmAllocationPolicy().allocateHostForVm(vm, targetHost);

    if (ack) {
      int[] data = new int[3];

      data[0] = getId();
      data[1] = vm.getId();

      if (result) {
        data[2] = CloudSimTags.TRUE;
      } else {
        data[2] = CloudSimTags.FALSE;
      }

      sendNow(vm.getUserId(), CloudSimTags.VM_CREATE_ACK, data);
    }

    if (result) {
      globalVmDatacenterMap.put(vm.getId(), this);

      getVmList().add(vm);

      if (vm.isBeingInstantiated()) {
        vm.setBeingInstantiated(false);
      }

      vm.updateVmProcessing(CloudSim.clock(), getVmAllocationPolicy().getHost(vm).getVmScheduler()
          .getAllocatedMipsForVm(vm));
    }

    return result;
  }

  private int getBackupIndexFromName(String vmName) {
    Pattern pattern = Pattern.compile("-backup-(\\d+)$");
    Matcher matcher = pattern.matcher(vmName);

    if (matcher.find()) {
      return Integer.parseInt(matcher.group(1));
    }

    return Integer.MAX_VALUE;
  }

  private void loadBackupMappings() {
    VmAllocationPolicyFromFile policy = (VmAllocationPolicyFromFile) getVmAllocationPolicy();
    Map<String, String> allPlacements = policy.getVmToHostMapping();

    for (String vmName : allPlacements.keySet()) {
      if (vmName.contains("-backup-")) {
        String originalVm = vmName.substring(0, vmName.indexOf("-backup-"));
        String hostName = allPlacements.get(vmName);
        Integer hostId = hostNameToId.get(hostName);

        if (hostId != null) {
          Host host = HostList.getById(getHostList(), hostId);

          if (host != null && !host.isFailed()) {
            vmBackups.computeIfAbsent(originalVm, k -> new ArrayList<>()).add(vmName);
            vmStatus.put(vmName, VmStatus.INACTIVE);
          } else {
            Log.printLine(CloudSim.clock() + ": Warning - Backup VM " + vmName +
                " mapped to invalid/failed host " + hostName);
          }
        }
      } else {
        vmStatus.put(vmName, VmStatus.ACTIVE);
      }
    }

    vmBackups.forEach((k, v) -> Collections.sort(v, Comparator.comparingInt(this::getBackupIndexFromName)));
  }

  private SDNVm findVmByName(List<? extends Vm> vmList, String vmName) {
    for (Vm vm : vmList) {
      SDNVm sdnVm = (SDNVm) vm;
      if (sdnVm.getName().equals(vmName)) {
        return sdnVm;
      }
    }
    return null;
  }

  public void printFailureMetrics(double finishTime) {
    double serviceAvailability = ((finishTime - totalServiceDowntime) / finishTime) * 100.0;

    Log.printLine("========== FAILURE METRICS ==========");
    Log.printLine(String.format("Service Availability: %.3f%%", serviceAvailability));
    Log.printLine("Total Simulation Time: " + finishTime);
    Log.printLine("Total Cloudlets Failed: " + String.format("%.0f", totalCloudletsFailed));
    Log.printLine("Total Processing Delay: " + totalProcessingDelay);
    Log.printLine("Total Transmission Delay: " + totalTransmissionDelay);
    Log.printLine("Total Delay: " + (totalProcessingDelay + totalTransmissionDelay));
    Log.printLine("Total Failures: " + totalFailures);
  }
}