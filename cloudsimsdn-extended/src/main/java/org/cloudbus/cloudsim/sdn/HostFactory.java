package org.cloudbus.cloudsim.sdn;

import org.cloudbus.cloudsim.sdn.physicalcomponents.SDNHost;

public interface HostFactory {
  public abstract SDNHost createHost(double mtbf, double mttr, int ram, long bw, long storage, long pes, double mips,
      String name);
}
