package org.cloudbus.cloudsim.sdn.example;

// import org.cloudbus.cloudsim.sdn.example.topogenerators.VirtualTopologyGeneratorVmTypesSFC;
import org.cloudbus.cloudsim.sdn.example.topogenerators.VirtualTopologyGeneratorVmTypesSFCSplitJoin;

public class SFCTopologyGenerator {
  // Configuration parameters
  private static final int NUM_WEB = 8; // Number of web servers
  private static final int NUM_APP = 24; // Number of app servers
  private static final int NUM_DB = 2; // Number of database servers
  private static final long LINK_BW = 1500000L; // Link bandwidth

  private static final String OUTPUT_FILE = "dataset-availability-sustainability/virtual-serial.json";

  // private static final String OUTPUT_FILE =
  // "dataset-availability-sustainability/virtual-split-join.json";

  public static void main(String[] args) {
    generateSFCTopology();
  }

  public static void generateSFCTopology() {
    try {
      // VirtualTopologyGeneratorVmTypesSFC generator = new
      // VirtualTopologyGeneratorVmTypesSFC();

      VirtualTopologyGeneratorVmTypesSFCSplitJoin generator = new VirtualTopologyGeneratorVmTypesSFCSplitJoin();

      boolean noScale = true; // Set to false if autoscaling is needed

      // Generate topology with configured parameters
      generator.generateLarge3TierTopologySFC(OUTPUT_FILE, noScale);

      System.out.println("SFC topology generated successfully in " + OUTPUT_FILE);
    } catch (Exception e) {
      System.err.println("Error generating SFC topology: " + e.getMessage());
      e.printStackTrace();
    }
  }

  // Helper method to validate configuration
  private static boolean validateConfig() {
    if (NUM_WEB <= 0 || NUM_APP <= 0 || NUM_DB <= 0) {
      System.err.println("Invalid number of servers configured");
      return false;
    }
    if (LINK_BW <= 0) {
      System.err.println("Invalid link bandwidth configured");
      return false;
    }
    return true;
  }
}