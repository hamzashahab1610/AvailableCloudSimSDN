package org.cloudbus.cloudsim.sdn.example;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.sdn.CloudSimEx;
import org.cloudbus.cloudsim.sdn.Configuration;
import org.cloudbus.cloudsim.sdn.SDNBroker;
import org.cloudbus.cloudsim.sdn.failure.Worst_SDNDatacenter;
import org.cloudbus.cloudsim.sdn.failure.RNC_SDNDatacenter;
import org.cloudbus.cloudsim.sdn.failure.RRC_SDNDatacenter;
import org.cloudbus.cloudsim.sdn.failure.RPC_SDNDatacenter;
import org.cloudbus.cloudsim.sdn.failure.Best_SDNDatacenter;
import org.cloudbus.cloudsim.sdn.monitor.power.PowerUtilizationMaxHostInterface;
import org.cloudbus.cloudsim.sdn.nos.NetworkOperatingSystem;
import org.cloudbus.cloudsim.sdn.parsers.PhysicalTopologyParser;
import org.cloudbus.cloudsim.sdn.physicalcomponents.switches.Switch;
import org.cloudbus.cloudsim.sdn.policies.selectlink.LinkSelectionPolicy;
import org.cloudbus.cloudsim.sdn.policies.selectlink.LinkSelectionPolicyBandwidthAllocation;
import org.cloudbus.cloudsim.sdn.policies.vmallocation.VmAllocationPolicyFromFile;
import org.cloudbus.cloudsim.sdn.workload.Workload;

public class Simulation {
  protected static String physicalTopologyFile = "dataset-energy/energy-physical.json";
  protected static String deploymentFile = "dataset-energy/energy-virtual.json";
  protected static String[] workload_files = {
      "dataset-energy/energy-workload.csv"
  };

  protected static List<String> workloads;
  private static boolean logEnabled = true;

  public interface VmAllocationPolicyFactory {
    public VmAllocationPolicy create(List<? extends Host> list);
  }

  @SuppressWarnings("unused")
  public static void main(String[] args) {
    CloudSimEx.setStartTime();

    workloads = new ArrayList<String>();

    // Parse system arguments
    if (args.length < 1) {
      System.exit(1);
    }

    if (args.length > 1)
      physicalTopologyFile = args[1];
    if (args.length > 2)
      deploymentFile = args[2];
    if (args.length > 3)
      for (int i = 3; i < args.length; i++) {
        workloads.add(args[i]);
      }
    else
      workloads = (List<String>) Arrays.asList(workload_files);

    printArguments(physicalTopologyFile, deploymentFile, workloads);

    Log.printLine("Starting CloudSim SDN...");

    try {
      // Initialize
      int num_user = 1; // number of cloud users
      Calendar calendar = Calendar.getInstance();
      boolean trace_flag = false; // mean trace events
      CloudSim.init(num_user, calendar, trace_flag);

      VmAllocationPolicyFactory vmAllocationFac = null;
      LinkSelectionPolicy ls = null;

      vmAllocationFac = new VmAllocationPolicyFactory() {
        public VmAllocationPolicy create(List<? extends Host> hostList) {
          // Serial - No Backup
          // return new VmAllocationPolicyFromFile(hostList,
          // "E:\\OneDrive - University of Regina\\University of
          // Regina\\Research\\AvailableCloudSimSDN\\python-simulator\\output\\serial\\No
          // Backup\\placements.txt");

          // Split-Join - No Backup
          // return new VmAllocationPolicyFromFile(hostList,
          // "E:\\OneDrive - University of Regina\\University of
          // Regina\\Research\\AvailableCloudSimSDN\\python-simulator\\output\\split-join\\No
          // Backup\\placements.txt");

          // Serial - Backup
          return new VmAllocationPolicyFromFile(hostList,
              "E:\\OneDrive - University of Regina\\University of Regina\\Research\\AvailableCloudSimSDN\\python-simulator\\output\\serial\\Backup\\placements.txt");

          // Split-Join - Backup
          // return new VmAllocationPolicyFromFile(hostList,
          // "E:\\OneDrive - University of Regina\\University of
          // Regina\\Research\\AvailableCloudSimSDN\\python-simulator\\output\\split-join\\Backup\\placements.txt");
        }
      };
      ls = new LinkSelectionPolicyBandwidthAllocation();

      Configuration.monitoringTimeInterval = Configuration.migrationTimeInterval = 1;

      // Create multiple Datacenters
      Map<NetworkOperatingSystem, RPC_SDNDatacenter> dcs = createPhysicalTopology(physicalTopologyFile, ls,
          vmAllocationFac);

      // Broker
      SDNBroker broker = createBroker();
      int brokerId = broker.getId();

      // Submit virtual topology
      for (RPC_SDNDatacenter dc : dcs.values()) {
        broker.submitDeployApplication(dc, deploymentFile);
      }

      // Submit individual workloads
      submitWorkloads(broker);

      // Sixth step: Starts the simulation
      if (!Simulation.logEnabled)
        Log.disable();

      startSimulation(broker, dcs.values());

    } catch (Exception e) {
      e.printStackTrace();
      Log.printLine("Unwanted errors happen");
    }
  }

  public static void startSimulation(SDNBroker broker, Collection<RPC_SDNDatacenter> dcs) {
    double finishTime = CloudSim.startSimulation();

    CloudSim.stopSimulation();

    Log.enable();

    // broker.printResult();

    Log.printLine(finishTime + ": ========== EXPERIMENT FINISHED ===========");

    // Print results when simulation is over
    List<Workload> wls = broker.getWorkloads();
    if (wls != null)
      LogPrinter.printWorkloadList(wls);

    // Print hosts' and switches' total utilization.
    List<Host> hostList = getAllHostList(dcs);
    List<Switch> switchList = getAllSwitchList(dcs);
    LogPrinter.printEnergyConsumption(hostList, switchList, finishTime);

    LogPrinter.printTotalEnergy();

    // Print failure metrics for each datacenter
    for (RPC_SDNDatacenter dc : dcs) {
      dc.printFailureMetrics(finishTime);
    }

    Log.printLine("Simultanously used hosts:" + maxHostHandler.getMaxNumHostsUsed());
    Log.printLine("CloudSim SDN finished!");
  }

  private static List<Switch> getAllSwitchList(Collection<RPC_SDNDatacenter> dcs) {
    List<Switch> allSwitch = new ArrayList<Switch>();
    for (RPC_SDNDatacenter dc : dcs) {
      allSwitch.addAll(dc.getNOS().getSwitchList());
    }

    return allSwitch;
  }

  private static List<Host> getAllHostList(Collection<RPC_SDNDatacenter> dcs) {
    List<Host> allHosts = new ArrayList<Host>();
    for (RPC_SDNDatacenter dc : dcs) {
      if (dc.getNOS().getHostList() != null)
        allHosts.addAll(dc.getNOS().getHostList());
    }

    return allHosts;
  }

  public static Map<NetworkOperatingSystem, RPC_SDNDatacenter> createPhysicalTopology(
      String physicalTopologyFile,
      LinkSelectionPolicy ls, VmAllocationPolicyFactory vmAllocationFac) {
    HashMap<NetworkOperatingSystem, RPC_SDNDatacenter> dcs = new HashMap<NetworkOperatingSystem, RPC_SDNDatacenter>();
    // This funciton creates Datacenters and NOS inside the data cetner.
    Map<String, NetworkOperatingSystem> dcNameNOS = PhysicalTopologyParser
        .loadPhysicalTopologyMultiDC(physicalTopologyFile);

    for (String dcName : dcNameNOS.keySet()) {
      NetworkOperatingSystem nos = dcNameNOS.get(dcName);
      nos.setLinkSelectionPolicy(ls);
      RPC_SDNDatacenter datacenter = createFailureSDNDatacenter(dcName, nos, vmAllocationFac);
      dcs.put(nos, datacenter);
    }
    return dcs;
  }

  public static void submitWorkloads(SDNBroker broker) {
    // Submit workload files individually
    if (workloads != null) {
      for (String workload : workloads) {
        // Check if this is a directory path
        if (workload.endsWith("/") || workload.endsWith("\\")) {
          submitWorkloadsFromDirectory(broker, workload);
        } else {
          broker.submitRequests(workload);
        }
      }
    }

    // Or, Submit groups of workloads
    // submitGroupWorkloads(broker, WORKLOAD_GROUP_NUM, WORKLOAD_GROUP_PRIORITY,
    // WORKLOAD_GROUP_FILENAME, WORKLOAD_GROUP_FILENAME_BG);
  }

  public static void printArguments(String physical, String virtual, List<String> workloads) {
    System.out.println("Data center infrastructure (Physical Topology) : " + physical);
    System.out.println("Virtual Machine and Network requests (Virtual Topology) : " + virtual);
    System.out.println("Workloads: ");
    for (String work : workloads)
      System.out.println("  " + work);
  }

  /**
   * Creates the datacenter.
   *
   * @param name the name
   *
   * @return the datacenter
   */
  protected static PowerUtilizationMaxHostInterface maxHostHandler = null;

  protected static RPC_SDNDatacenter createFailureSDNDatacenter(String name, NetworkOperatingSystem nos,
      VmAllocationPolicyFactory vmAllocationFactory) {
    // In order to get Host information, pre-create NOS.
    List<Host> hostList = nos.getHostList();

    String arch = "x86"; // system architecture
    String os = "Linux"; // operating system
    String vmm = "Xen";

    double time_zone = 10.0; // time zone this resource located
    double cost = 3.0; // the cost of using processing in this resource
    double costPerMem = 0.05; // the cost of using memory in this resource
    double costPerStorage = 0.001; // the cost of using storage in this
                                   // resource
    double costPerBw = 0.0; // the cost of using bw in this resource
    LinkedList<Storage> storageList = new LinkedList<Storage>(); // we are not adding SAN
    // devices by now

    DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
        arch, os, vmm, hostList, time_zone, cost, costPerMem,
        costPerStorage, costPerBw);

    // Create Datacenter with previously set parameters
    RPC_SDNDatacenter datacenter = null;
    try {
      VmAllocationPolicy vmPolicy = null;

      if (hostList.size() != 0) {
        vmPolicy = vmAllocationFactory.create(hostList);
        maxHostHandler = (PowerUtilizationMaxHostInterface) vmPolicy;
        datacenter = new RPC_SDNDatacenter(name, characteristics, vmPolicy, storageList, 0, nos);
      }

      nos.setDatacenter(datacenter);
    } catch (Exception e) {
      e.printStackTrace();
    }

    return datacenter;
  }

  // We strongly encourage users to develop their own broker policies, to
  // submit vms and cloudlets according
  // to the specific rules of the simulated scenario
  /**
   * Creates the broker.
   *
   * @return the datacenter broker
   */
  protected static SDNBroker createBroker() {
    SDNBroker broker = null;
    try {
      broker = new SDNBroker("Broker");
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
    return broker;
  }

  static String WORKLOAD_GROUP_FILENAME = "workload_10sec_100_default.csv"; // group 0~9
  static String WORKLOAD_GROUP_FILENAME_BG = "workload_10sec_100.csv"; // group 10~29
  static int WORKLOAD_GROUP_NUM = 50;
  static int WORKLOAD_GROUP_PRIORITY = 1;

  public static void submitGroupWorkloads(SDNBroker broker, int workloadsNum, int groupSeperateNum,
      String filename_suffix_group1, String filename_suffix_group2) {
    for (int set = 0; set < workloadsNum; set++) {
      String filename = filename_suffix_group1;
      if (set >= groupSeperateNum)
        filename = filename_suffix_group2;

      filename = set + "_" + filename;
      broker.submitRequests(filename);
    }
  }

  public static void submitWorkloadsFromDirectory(SDNBroker broker, String workloadDir) {
    // Create File object for the workloads directory
    // File workloadsFolder = new File(Configuration.workingDirectory +
    // workloadDir);
    File workloadsFolder = new File(workloadDir);

    if (!workloadsFolder.isAbsolute()) {
      workloadsFolder = new File(Configuration.workingDirectory, workloadDir);
    }

    if (!workloadsFolder.exists() || !workloadsFolder.isDirectory()) {
      System.err.println("Invalid workload directory: " + workloadsFolder.getAbsolutePath());
      return;
    }

    // List all files in directory that end with .csv
    File[] workloadFiles = workloadsFolder.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));

    if (workloadFiles == null || workloadFiles.length == 0) {
      System.out.println("No workload files found in: " + workloadsFolder.getAbsolutePath());
      return;
    }

    // Submit each workload file
    for (File file : workloadFiles) {
      String workloadPath = workloadDir + file.getName();
      System.out.println("Submitting workload: " + workloadPath);
      broker.submitRequests(workloadPath);
    }
  }
}
