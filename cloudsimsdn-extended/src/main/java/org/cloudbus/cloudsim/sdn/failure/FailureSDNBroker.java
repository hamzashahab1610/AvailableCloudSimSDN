package org.cloudbus.cloudsim.sdn.failure;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;
import org.cloudbus.cloudsim.sdn.SDNBroker;
import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNDatacenter;
import org.cloudbus.cloudsim.sdn.virtualcomponents.SDNVm;

public class FailureSDNBroker extends SDNBroker {
  private static final int VM_FAILURE_EVENT = 9997;

  private FailureGenerator vmFailureGenerator;
  private Map<String, Integer> vmNameToId;
  private List<FailureGenerator.Event> vmFailureEvents;

  public FailureSDNBroker(String name) throws Exception {
    super(name);
    this.vmNameToId = new HashMap<>();
    this.vmFailureGenerator = new FailureGenerator("dataset-availability-sustainability/vm_events.csv", "vm");
    this.vmFailureEvents = vmFailureGenerator.getEvents();
  }

  @Override
  protected void applicationSubmitCompleted(SimEvent ev) {
    super.applicationSubmitCompleted(ev);

    populateVmNameMapping();

    if (vmFailureEvents == null || vmFailureEvents.isEmpty())
      return;

    scheduleFailureEvents();
  }

  private void populateVmNameMapping() {
    for (SDNDatacenter dc : datacenters.values()) {
      List<Vm> vmList = dc.getVmList();
      for (Vm vm : vmList) {
        if (vm instanceof SDNVm) {
          SDNVm sdnVm = (SDNVm) vm;
          vmNameToId.put(sdnVm.getName(), vm.getId());
        }
      }
    }
  }

  private void scheduleFailureEvents() {
    double currentTime = CloudSim.clock();

    int totalVms = 0;
    for (SDNDatacenter dc : datacenters.values()) {
      totalVms += dc.getVmList().size();
    }

    Set<String> availableVmNames = new HashSet<>(vmNameToId.keySet());

    Set<String> scheduledVms = new HashSet<>();
    int eventCount = 0;

    Log.printLine(CloudSim.clock() + ": Scheduling VM failures for " + totalVms + " VMs");

    for (FailureGenerator.Event event : vmFailureEvents) {
      if (eventCount >= totalVms) {
        break;
      }

      if (scheduledVms.contains(event.nodeID)) {
        continue;
      }

      if (!availableVmNames.contains(event.nodeID)) {
        continue;
      }

      Integer vmId = vmNameToId.get(event.nodeID);

      if (vmId == null) {
        Log.printLine(CloudSim.clock() + ": Warning - Could not find VM ID for machine " + event.nodeID);
        continue;
      }

      SDNVm sdnVm = null;

      for (SDNDatacenter dc : datacenters.values()) {
        Vm vm = VmList.getById(dc.getVmList(), vmId);
        if (vm != null) {
          sdnVm = (SDNVm) vm;
          break;
        }
      }

      if (sdnVm == null) {
        Log.printLine(CloudSim.clock() + ": Warning - Could not find VM object for ID " + vmId);
        continue;
      }

      if (!sdnVm.getName().contains("-backup-")) {
        double scheduleTime = currentTime + event.timestamp;
        send(3, scheduleTime, VM_FAILURE_EVENT, sdnVm);

        Log.printLine(String.format("%f: Scheduled %s event for VM %s(ID:%d) at %f",
            CloudSim.clock(), "failure", event.nodeID, vmId, scheduleTime));

        scheduledVms.add(event.nodeID);
        eventCount++;
      }
    }

    Log.printLine(CloudSim.clock() + ": Successfully scheduled " + eventCount + " VM failure events");
  }
}
