package org.cloudbus.cloudsim.sdn.failure;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNHost;

import java.io.*;
import java.util.*;

public class FailurePredictor extends SimEntity {
  private static final String PYTHON_SCRIPT = "dataset-availability-sustainability/make_prediction.py";
  private static final double PREDICTION_INTERVAL = 10.0;
  private static final int FAILURE_PREDICTION_NOTIFICATION = 9995;
  private static final int MONITOR_BATCH_EVENT = 9994;

  private static final int BATCH_SIZE = 100; // Process 100 hosts at once
  private static final double BATCH_INTERVAL = 0.5;

  private final int datacenterId;
  private final Map<Integer, SDNHost> monitoredHosts = new HashMap<>();
  private final Map<Integer, Boolean> lastPredictions = new HashMap<>();

  private List<Integer> hostIdList = new ArrayList<>();
  private int currentBatchIndex = 0;

  public FailurePredictor(int datacenterId) {
    super("FailurePredictor");
    this.datacenterId = datacenterId;
  }

  @Override
  public void startEntity() {
    Log.printLine(CloudSim.clock() + ": FailurePredictor started with " + monitoredHosts.size() + " hosts");

    if (!monitoredHosts.isEmpty()) {
      hostIdList = new ArrayList<>(monitoredHosts.keySet());
      currentBatchIndex = 0;
      send(getId(), PREDICTION_INTERVAL, MONITOR_BATCH_EVENT);
    }
  }

  public void registerHost(SDNHost host) {
    monitoredHosts.put(host.getId(), host);
    lastPredictions.put(host.getId(), false);
  }

  @Override
  public void processEvent(SimEvent ev) {
    if (ev.getTag() == MONITOR_BATCH_EVENT) {
      processBatch();
    }
  }

  private void processBatch() {
    double currentTime = CloudSim.clock();

    // Get batch of hosts to check
    int endIndex = Math.min(currentBatchIndex + BATCH_SIZE, hostIdList.size());
    List<SDNHost> batchHosts = new ArrayList<>();
    List<Integer> batchHostIds = new ArrayList<>();

    for (int i = currentBatchIndex; i < endIndex; i++) {
      int hostId = hostIdList.get(i);
      SDNHost host = monitoredHosts.get(hostId);

      if (host != null && !host.isFailed()) {
        batchHosts.add(host);
        batchHostIds.add(hostId);
      }
    }

    // Predict for entire batch at once
    if (!batchHosts.isEmpty()) {
      Map<Integer, Boolean> predictions = predictBatch(batchHosts, batchHostIds, currentTime);

      for (Map.Entry<Integer, Boolean> entry : predictions.entrySet()) {
        int hostId = entry.getKey();
        boolean willFail = entry.getValue();
        boolean previousPrediction = lastPredictions.get(hostId);

        if (willFail && !previousPrediction) {
          Log.printLine(currentTime + ": FailurePredictor - Host " + hostId + " will fail soon!");
          send(datacenterId, 0.0, FAILURE_PREDICTION_NOTIFICATION, hostId);
        }

        lastPredictions.put(hostId, willFail);
      }
    }

    currentBatchIndex = endIndex;

    // Schedule next batch or reset
    if (currentBatchIndex >= hostIdList.size()) {
      currentBatchIndex = 0;
      send(getId(), PREDICTION_INTERVAL, MONITOR_BATCH_EVENT);
      Log.printLine(currentTime + ": FailurePredictor completed full check cycle");
    } else {
      send(getId(), BATCH_INTERVAL, MONITOR_BATCH_EVENT);
    }
  }

  private Map<Integer, Boolean> predictBatch(List<SDNHost> hosts, List<Integer> hostIds, double currentTime) {
    Map<Integer, Boolean> results = new HashMap<>();
    File tempFile = null;

    try {
      tempFile = createBatchFeatureFile(hosts, hostIds, currentTime);
      String output = runPythonScript(tempFile.getAbsolutePath());

      // Parse results (expecting one line per host: "hostId,prediction")
      String[] lines = output.trim().split("\n");
      for (String line : lines) {
        String[] parts = line.split(",");
        if (parts.length == 2) {
          int hostId = Integer.parseInt(parts[0]);
          boolean prediction = "1".equals(parts[1]);
          results.put(hostId, prediction);
        }
      }

    } catch (Exception e) {
      Log.printLine("Batch prediction error: " + e.getMessage());
      // Return false for all hosts on error
      for (int hostId : hostIds) {
        results.put(hostId, false);
      }
    } finally {
      if (tempFile != null && tempFile.exists()) {
        tempFile.delete();
      }
    }

    return results;
  }

  private File createBatchFeatureFile(List<SDNHost> hosts, List<Integer> hostIds, double time) throws IOException {
    File tempFile = File.createTempFile("batch_hosts_", ".csv");

    try (PrintWriter writer = new PrintWriter(tempFile)) {
      writer.println("host_id,cpus,memory,platform_id,cluster,time");

      for (int i = 0; i < hosts.size(); i++) {
        SDNHost host = hosts.get(i);
        int hostId = hostIds.get(i);

        writer.println(String.format("%d,%d,%d,%s,%s,%.2f",
            hostId,
            host.getNumberOfPes(),
            host.getRam(),
            host.getName().substring(0, 1),
            host.getName(),
            time));
      }
    }

    return tempFile;
  }

  private String runPythonScript(String featureFile) throws Exception {
    ProcessBuilder pb = new ProcessBuilder("python", PYTHON_SCRIPT, featureFile);
    pb.redirectErrorStream(true);

    Process process = pb.start();

    StringBuilder output = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        output.append(line).append("\n");
      }
    }

    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new RuntimeException("Python script failed with code " + exitCode);
    }

    return output.toString();
  }

  @Override
  public void shutdownEntity() {
    Log.printLine(CloudSim.clock() + ": FailurePredictor shutdown");
    monitoredHosts.clear();
    lastPredictions.clear();
    hostIdList.clear();
  }
}