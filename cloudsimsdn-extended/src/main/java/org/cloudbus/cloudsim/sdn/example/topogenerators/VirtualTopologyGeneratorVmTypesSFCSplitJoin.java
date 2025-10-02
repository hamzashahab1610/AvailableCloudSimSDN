package org.cloudbus.cloudsim.sdn.example.topogenerators;

import java.util.ArrayList;
import java.util.List;

public class VirtualTopologyGeneratorVmTypesSFCSplitJoin extends VirtualTopologyGeneratorVmTypes {

  public static void main(String[] argv) {
    VirtualTopologyGeneratorVmTypesSFCSplitJoin vmGenerator = new VirtualTopologyGeneratorVmTypesSFCSplitJoin();
    boolean noscale = true;
    vmGenerator.generateLarge3TierTopologySFC("split-join.virtual.json", noscale);
  }

  public void generateLarge3TierTopologySFC(String jsonFileName, boolean noscale) {
    final int numVideoSources = 3;
    final int numAudioTranscoders = 3;
    final int numVideoTranscoders = 3;
    final int numInetAPs = 3;

    final int groupNum = 1;
    final Long linkBW = 1500000L;

    for (int vmGroupId = 0; vmGroupId < groupNum; vmGroupId++) {
      TimeGen startTime = new TimeGen(-1);
      TimeGen endTime = new TimeGen(-1);

      generateVMGroupComplex(numVideoSources, numAudioTranscoders, numVideoTranscoders, numInetAPs,
          startTime, endTime, linkBW, vmGroupId, noscale);
    }

    wrtieJSON(jsonFileName);
  }

  int vmNum = 0;

  enum VMtype {
    Video,
    InetAP,
    AudioTranscoder,
    VideoTranscoder,
  }

  public VMSpec createVM(VMtype vmtype, double startTime, double endTime, int vmGroupId, int vmGroupSubId, long vmBW) {
    String name = "vm";
    int pes = 1;
    long vmSize = 1000;
    long mips = 10000;
    int vmRam = 256;
    // long vmBW=120000000;
    vmBW = 100000000;

    switch (vmtype) {
      case Video:
        mips = 15000;
        pes = 6;
        vmRam = 512;
        name = "video";
        break;
      case InetAP:
        mips = 12000;
        pes = 4;
        vmRam = 256;
        name = "inetap";
        break;
      case AudioTranscoder:
        mips = 12000;
        pes = 4;
        vmRam = 256;
        name = "audiotranscoder";
        break;
      case VideoTranscoder:
        mips = 25000;
        pes = 12;
        vmRam = 512;
        name = "videotranscoder";
        break;
    }

    name += vmGroupId;
    if (vmGroupSubId != -1) {
      name += "-" + vmGroupSubId;
    }
    vmNum++;

    VMSpec vm = addVM(name, pes, mips, vmRam, vmSize, vmBW, startTime, endTime);
    return vm;
  }

  public void generateVMGroupComplex(int numVideoSources, int numAudioTranscoders, int numVideoTranscoders,
      int numInetAPs,
      TimeGen startTime, TimeGen endTime, Long linkBw,
      int groupId, boolean noscale) {
    System.out.printf("Generating Media Processing Group(%d)\n", groupId);

    VMSpec[] videoSources = new VMSpec[numVideoSources];
    VMSpec[] audioTranscoders = new VMSpec[numAudioTranscoders];
    VMSpec[] videoTranscoders = new VMSpec[numVideoTranscoders];
    VMSpec[] inetAPs = new VMSpec[numInetAPs];

    // Create VMs
    for (int i = 0; i < numVideoSources; i++)
      videoSources[i] = this.createVM(VMtype.Video, startTime.getStartTime(), endTime.getEndTime(), groupId, i,
          linkBw);

    for (int i = 0; i < numAudioTranscoders; i++)
      audioTranscoders[i] = this.createVM(VMtype.AudioTranscoder, startTime.getStartTime(), endTime.getEndTime(),
          groupId, i, linkBw);

    for (int i = 0; i < numVideoTranscoders; i++)
      videoTranscoders[i] = this.createVM(VMtype.VideoTranscoder, startTime.getStartTime(), endTime.getEndTime(),
          groupId, i, linkBw);

    for (int i = 0; i < numInetAPs; i++)
      inetAPs[i] = this.createVM(VMtype.InetAP, startTime.getStartTime(), endTime.getEndTime(), groupId, i, linkBw);

    // Add links between VMs
    long linkBwPerCh = linkBw / 2;

    if (noscale)
      linkBwPerCh = 2000000;

    if (linkBw > 0) {
      int maxNum = Integer.max(numVideoSources, numInetAPs);
      for (int i = 0; i < maxNum; i++) {
        addLinkAutoNameBoth(videoSources[i % numVideoSources], audioTranscoders[i % numAudioTranscoders], linkBwPerCh);
        addLinkAutoNameBoth(videoSources[i % numVideoSources], videoTranscoders[i % numVideoTranscoders], linkBwPerCh);
        addLinkAutoNameBoth(audioTranscoders[i % numAudioTranscoders], inetAPs[i % numInetAPs], linkBwPerCh);
        addLinkAutoNameBoth(videoTranscoders[i % numVideoTranscoders], inetAPs[i % numInetAPs], linkBwPerCh);
      }
    }

    // Create Service Functions directly as VMs
    createSplitJoinSFCPolicy(videoSources, audioTranscoders, videoTranscoders, inetAPs, startTime, endTime, linkBw,
        noscale);
  }

  public void createSplitJoinSFCPolicy(VMSpec[] videoSources, VMSpec[] audioTranscoders, VMSpec[] videoTranscoders,
      VMSpec[] inetAPs,
      TimeGen startTime, TimeGen endTime, Long linkBw, boolean noscale) {
    int avSplitNum = 2;
    int multiplexNum = 2;

    if (noscale) {
      avSplitNum = 2;
      multiplexNum = 2;
    }

    // Create Service Functions (only Split and Multiplex are VNFs)
    SFSpec[] avSplits = new SFSpec[avSplitNum];
    for (int i = 0; i < avSplitNum; i++) {
      avSplits[i] = addSFAVSplit("avsplit" + i, linkBw, startTime, endTime, noscale);
    }

    SFSpec[] multiplexes = new SFSpec[multiplexNum];
    for (int i = 0; i < multiplexNum; i++) {
      multiplexes[i] = addSFMultiplex("multiplex" + i, linkBw, startTime, endTime, noscale);
    }

    // Create Split-and-Join Chains
    createSplitJoinChains(videoSources, audioTranscoders, videoTranscoders, inetAPs, avSplits, multiplexes);
  }

  private void createSplitJoinChains(VMSpec[] videoSources, VMSpec[] audioTranscoders, VMSpec[] videoTranscoders,
      VMSpec[] inetAPs, SFSpec[] avSplits, SFSpec[] multiplexes) {

    // Create the specific chains you requested:
    // 1. Video (VM) -> Split (VNF) -> Audio Transcoder (VM)
    {
      List<SFSpec>[] audioFirstChains = createSingleStepChain(avSplits);
      double expTime = 2.0;
      // This creates policies from Video VMs through Split VNFs to Audio Transcoder
      // VMs
      addSFCPolicyCollective(videoSources, audioTranscoders, audioFirstChains, expTime);
    }

    // 2. Audio Transcoder (VM) -> Multiplex (VNF) -> InetAP (VM)
    {
      List<SFSpec>[] audioSecondChains = createSingleStepChain(multiplexes);
      double expTime = 2.5;
      // This creates policies from Audio Transcoder VMs through Multiplex VNFs to
      // InetAP VMs
      addSFCPolicyCollective(audioTranscoders, inetAPs, audioSecondChains, expTime);
    }

    // 3. Video (VM) -> Split (VNF) -> Video Transcoder (VM)
    {
      List<SFSpec>[] videoFirstChains = createSingleStepChain(avSplits);
      double expTime = 2.5;
      // This creates policies from Video VMs through Split VNFs to Video Transcoder
      // VMs
      addSFCPolicyCollective(videoSources, videoTranscoders, videoFirstChains, expTime);
    }

    // 4. Video Transcoder (VM) -> Multiplex (VNF) -> InetAP (VM)
    {
      List<SFSpec>[] videoSecondChains = createSingleStepChain(multiplexes);
      double expTime = 3.0;
      // This creates policies from Video Transcoder VMs through Multiplex VNFs to
      // InetAP VMs
      addSFCPolicyCollective(videoTranscoders, inetAPs, videoSecondChains, expTime);
    }
  }

  private List<SFSpec>[] createSingleStepChain(SFSpec[] singleSF) {
    @SuppressWarnings("unchecked")
    List<SFSpec>[] chains = new List[singleSF.length];
    for (int i = 0; i < singleSF.length; i++) {
      chains[i] = new ArrayList<SFSpec>();
      chains[i].add(singleSF[i]);
    }
    return chains;
  }

  public void addSFCPolicyCollective(VMSpec[] srcList, VMSpec[] dstList, List<SFSpec>[] sfChains,
      double expectedTime) {
    int maxNum = Integer.max(srcList.length, dstList.length);
    for (int i = 0; i < maxNum; i++) {
      VMSpec src = srcList[i % srcList.length];
      VMSpec dest = dstList[i % dstList.length];
      List<SFSpec> sfChain = sfChains[i % sfChains.length];
      String linkname = getAutoLinkName(src, dest);
      String policyname = "sfc-" + linkname;

      addSFCPolicy(policyname, src, dest, linkname, sfChain, expectedTime);
    }
  }

  public SFSpec addSFAVSplit(String name, long linkBw, TimeGen startTime, TimeGen endTime, boolean noscale) {
    int pes = 4;
    if (noscale)
      pes = 8;
    long mips = 15000;
    int ram = 16;
    long storage = 16;
    long bw = linkBw;
    long miPerOperation = 500;
    SFSpec sf = addSF(name, pes, mips, ram, storage, bw, startTime.getStartTime(), endTime.getEndTime(),
        miPerOperation, "AVSplit");
    return sf;
  }

  public SFSpec addSFMultiplex(String name, long linkBw, TimeGen startTime, TimeGen endTime, boolean noscale) {
    int pes = 3;
    if (noscale)
      pes = 6;
    long mips = 10000;
    int ram = 12;
    long storage = 12;
    long bw = linkBw;
    long miPerOperation = 300;
    SFSpec sf = addSF(name, pes, mips, ram, storage, bw, startTime.getStartTime(), endTime.getEndTime(),
        miPerOperation, "Multiplex");
    return sf;
  }
}