package org.cloudbus.cloudsim.sdn.policies.vmallocation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNHost;
import org.cloudbus.cloudsim.sdn.virtualcomponents.SDNVm;

public class VmAllocationPolicyFromFile extends VmAllocationPolicyEx {
  private String placementsFile;
  private Map<String, String> vmToHostMapping;

  public VmAllocationPolicyFromFile(List<? extends Host> hostList, String placementsFile) {
    super(hostList, null, null);
    vmToHostMapping = new HashMap<>();
    this.placementsFile = placementsFile;

    loadPlacementsFromFile(placementsFile);
  }

  public void loadPlacementsFromFile(String filename) {
    try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
      String line;
      while ((line = br.readLine()) != null) {
        // Skip comments and empty lines
        if (line.trim().startsWith("#") || line.trim().isEmpty()) {
          continue;
        }

        // Expected format: vmName,hostName
        String[] parts = line.split(",");

        if (parts.length == 2) {
          String vmName = parts[0].trim();
          String hostName = parts[1].trim();
          vmToHostMapping.put(vmName, hostName);
          Log.printLine("Loaded mapping: VM " + vmName + " -> Host " + hostName);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Error reading placements file: " + filename, e);
    }
  }

  @Override
  public boolean allocateHostForVm(Vm vm) {
    SDNVm sdnVm = (SDNVm) vm;
    String vmName = sdnVm.getName();

    if (vmToHostMapping.containsKey(vmName)) {
      String targetHostId = vmToHostMapping.get(vmName);

      for (Host host : getHostList()) {
        SDNHost sdnHost = (SDNHost) host;

        if (sdnHost.getName().equals(targetHostId)) {
          if (allocateHostForVm(vm, host)) {
            System.out.println("VM " + vmName + " allocated to Host " + targetHostId);
            return true;
          } else {
            System.err
                .println("Failed to allocate VM " + vmName + " to Host " + targetHostId + " (insufficient resources?)");
            return false;
          }
        }
      }
      System.err.println("Host " + targetHostId + " not found for VM " + vmName);
      return false;
    } else {
      System.err.println("No placement found for VM " + vmName);
      return false;
    }
  }

  public String getPlacementsFile() {
    return placementsFile;
  }

  public Map<String, String> getVmToHostMapping() {
    return vmToHostMapping;
  }
}