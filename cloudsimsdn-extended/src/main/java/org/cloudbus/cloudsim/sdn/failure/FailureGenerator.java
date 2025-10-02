package org.cloudbus.cloudsim.sdn.failure;

import java.io.*;
import java.util.*;

import org.cloudbus.cloudsim.sdn.policies.vmallocation.VmAllocationPolicyFromFile;

public class FailureGenerator {
  private String filepath;
  private Set<String> activeHosts;
  private String failureGeneratorType;

  public int FAILURE_EVENT;
  public int RECOVERY_EVENT;

  private static final double MICRO_TO_SECONDS = 1.0e-6;
  private static final long TIME_OFFSET_SECONDS = 600;
  // private final double compressionFactor = 0.0001;
  private final double compressionFactor = 0.001;

  // Event structure
  public class Event {
    public double timestamp;
    public String nodeID;
    public int eventType;

    public Event(long microTime, String machine, int type) {
      this.timestamp = ((microTime * MICRO_TO_SECONDS) - TIME_OFFSET_SECONDS) * compressionFactor;
      this.nodeID = machine;
      this.eventType = type;
    }
  }

  private List<Event> events;

  public FailureGenerator(String filepath, String failureGeneratorType) {
    this.filepath = filepath;
    this.failureGeneratorType = failureGeneratorType;
    this.events = new ArrayList<>();

    this.FAILURE_EVENT = (this.failureGeneratorType.equals("vm") ? 5 : 2);
    this.RECOVERY_EVENT = (this.failureGeneratorType.equals("vm") ? 3 : 1);

    parseTrace();
  }

  public FailureGenerator(String filepath, VmAllocationPolicyFromFile vmAllocation, String failureGeneratorType) {
    this.filepath = filepath;
    this.failureGeneratorType = failureGeneratorType;
    this.events = new ArrayList<>();

    this.FAILURE_EVENT = (this.failureGeneratorType.equals("vm") ? 5 : 2);
    this.RECOVERY_EVENT = (this.failureGeneratorType.equals("vm") ? 3 : 1);

    this.activeHosts = new HashSet<>(vmAllocation.getVmToHostMapping().values());
    parseTrace();
  }

  private void parseTrace() {
    try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
      String line;
      boolean isFirstLine = true;

      while ((line = br.readLine()) != null) {
        if (isFirstLine) {
          isFirstLine = false;
          continue;
        }

        if (line.startsWith("#") || line.startsWith("//"))
          continue; // Skip comments

        String[] parts = line.split(",");

        if (parts.length >= 3) {
          String nodeID = parts[0].trim();

          // Only process events for machines that have VMs allocated
          if (activeHosts != null && !activeHosts.isEmpty() && !activeHosts.contains(nodeID)) {
            continue;
          }

          long microTime = Long.parseLong(parts[1].trim());
          int eventType = Integer.parseInt(parts[2].trim());

          events.add(new Event(microTime, nodeID, eventType));
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public List<Event> getEvents() {
    return events;
  }
}