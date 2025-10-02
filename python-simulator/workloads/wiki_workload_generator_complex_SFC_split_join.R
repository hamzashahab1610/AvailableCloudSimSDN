library(actuar)
library(pracma)
B=1
KB=B*1024
MB=KB*1024

MAX_CPU_MI = 20000

options(scipen = 500)

NETWORK_PACKET_SIZE_MULTIPLY = 1
CPU_MULTIPLY  = 1
NUM_WORKLOAD_RATE_SCALE = 1

PACKETSIZE_VIDEO_AUDIOTRANS = 1500
PACKETSIZE_VIDEO_VIDEOTRANS = 1500
PACKETSIZE_AUDIOTRANS_INETAP = 65000
PACKETSIZE_VIDEOTRANS_INETAP = 65000

CPUSIZE_VIDEO = 72 * CPU_MULTIPLY
CPUSIZE_AUDIOTRANS = 306 * CPU_MULTIPLY
CPUSIZE_VIDEOTRANS = 585 * CPU_MULTIPLY
CPUSIZE_INETAP = 67 * CPU_MULTIPLY

rlnorm2 <-function(n,u,s) {
  r = rnorm(n, mean=u, sd=s)
  return(2^r)
}

generate_atimes_lnorm<-function(n,u,s, scale=1000) {
  r <- rlnorm2(n, u, s)/scale
  return(round(cumsum(r), digits=4))
}


generate_atimes_dur<-function(u,s, target.rate = -1, dur=1, scale=1) {
  n = target.rate*dur*1.5
  
  times = generate_atimes_lnorm(n,u,s,scale)
  if(target.rate != -1) {
    throughput = n / tail(times,n=1)
    ratio = throughput / target.rate
    times = times * ratio
  }
  
  times=times[times<=dur]
    
  return(round(times, digits=4))
}

generate_repeat<-function(n,mean) {
  sd = mean*0.2
  r <- rnorm(n, mean, sd)
  return(round(r))
}

generate_packet_size<-function(n,u,s, scale) {
  r <- rlnorm2(n, u, s) * scale
  cat("median packet size:",  median(r), "\n")
  
  return(round(r))
}

generate_cpu_size<-function(n, location, shape, vm_cap=1) {  
  r = rpareto(n*2, shape=shape, scale=location)
  r = round(r*vm_cap)
  r = r[r < MAX_CPU_MI]
  
  while (length(r) < n) {
    rn = rpareto(n, shape = shape, scale = location)
    rn = round(rn * vm_cap)
    r = c(r, rn[rn < MAX_CPU_MI])
  }
  
  r = r[1:n]
  
  return(r)
}

combine_works <- function(atime, name.1, name.2, name.3,
                          w.1.1, p.1.2, link.1.2,
                          w.2.1, p.2.3, link.2.3,
                          w.3) {
  zeros = rep(0, length(atime))

  wl.0.1 = cbind(name.1, zeros, w.1.1)
  wl.1.2 = cbind(link.1.2, name.2, p.1.2, w.2.1)
  wl.2.3 = cbind(link.2.3, name.3, p.2.3, w.3)

  workload = cbind(atime, wl.0.1, wl.1.2, wl.2.3)

  return(workload)
}

# Add use_videotrans parameter to create_workload function
create_workload<-function(n = -1, durtime = -1, 
                          target.rate=-1, starttime=0, seed = 10, filename="workloads.csv", 
                          name1set, name2set, name3set, 
                          is.dedicated=FALSE, is.append = FALSE, use_videotrans=FALSE) {
  t.user.front.mean = 1.56270
  t.user.front.sd = 1.5458
  
  t.user.front.mean = 0.43154
  t.user.front.sd = 1.1225

  w.video.location = 67.729
  w.video.shape = 1.1108

  w.audiotrans.location = 67.8342
  w.audiotrans.shape = 0.9822

  w.videotrans.location = 585  # Add videotranscoder parameters
  w.videotrans.shape = 1.2

  w.inetap.location = 49.2665
  w.inetap.shape = 1.4648

  w.1.1.location = w.video.location
  if(use_videotrans) {
    w.2.1.location = w.videotrans.location
    w.2.1.shape = w.videotrans.shape
  } else {
    w.2.1.location = w.audiotrans.location
    w.2.1.shape = w.audiotrans.shape
  }
  w.3.location = w.inetap.location
  
  w.1.1.shape = w.video.shape
  w.3.shape = w.inetap.shape

  scale_factor_net = NETWORK_PACKET_SIZE_MULTIPLY

  t.user.front.mean = 0.59481
  t.user.front.sd = 1.5284
  
  set.seed(seed)
  if( durtime != -1) {
    t.user.front = generate_atimes_dur(t.user.front.mean, t.user.front.sd, target.rate/durtime, dur=durtime)
    n=length(t.user.front)
  } else {
    t.user.front = generate_atimes(n, t.user.front.mean, t.user.front.sd, target.rate)
    durtime = tail(t.user.front, n=1)
  }
  
  if(n == 0L)
    return(NA)
  t.user.front = t.user.front + starttime

  # Use appropriate packet sizes based on transcoder type
  if(use_videotrans) {
    psize.front.mid = round(generate_repeat(n, PACKETSIZE_VIDEO_VIDEOTRANS)*scale_factor_net)
    psize.mid.back  = round(generate_repeat(n, PACKETSIZE_VIDEOTRANS_INETAP)*scale_factor_net)
  } else {
    psize.front.mid = round(generate_repeat(n, PACKETSIZE_VIDEO_AUDIOTRANS)*scale_factor_net)
    psize.mid.back  = round(generate_repeat(n, PACKETSIZE_AUDIOTRANS_INETAP)*scale_factor_net)
  }
  
  set.seed(seed+5)
  w.1.1=generate_cpu_size(n=n, location=w.1.1.location, shape=w.1.1.shape)
  set.seed(seed+6)
  w.2.1=generate_cpu_size(n=n, location=w.2.1.location, shape=w.2.1.shape)
  set.seed(seed+7)
  w.3=generate_cpu_size(n=n, location=w.3.location, shape=w.3.shape)
  
  # Prepare server names (webX-Y, appX-Y, dbX-Y)
  name1=rep_len(name1set, n)
  name2=rep_len(name2set, n)
  name3=rep_len(name3set, n)
  
  link.1.2 = link.2.3 = "default"

  if(is.dedicated) {
    link.1.2 = paste(name1,name2, sep="")
    link.2.3 = paste(name2,name3, sep="")
  }
    
  workload= combine_works(t.user.front, name1, name2, name3, 
                            w.1.1, psize.front.mid, link.1.2,
                            w.2.1, psize.mid.back, link.2.3,
                            w.3)
  
  return(workload)
}

is.list.empty <- function(workload_list, index) {
  return(is.na(workload_list[[index]][1]))
}

# Modify create_historical_data to generate both workload types in same file
create_historical_data<-function(langId, timelist, ratelist, videoNum, audioTransNum, inetapNum,
                                 startTime=0, endTime=.Machine$integer.max,
                                 use.dedicated.network = TRUE, fileId=-1
                                 ) {
  if(fileId == -1)
    fileId = langId
  
  is_append=FALSE
  interevent = timelist[2] - timelist[1]

  maxNum = Lcm(videoNum, Lcm(audioTransNum, inetapNum))
  videoSeq = rep_len(seq(0,videoNum-1), maxNum)
  audioTransSeq = rep_len(seq(0,audioTransNum-1), maxNum)
  videoTransSeq = rep_len(seq(0,audioTransNum-1), maxNum)  # Add videotranscoder sequence
  inetapSeq = rep_len(seq(0, inetapNum - 1), maxNum)
  
  videoNamePool = paste(sprintf("video%d-", langId), videoSeq, sep="")
  audioTransNamePool = paste(sprintf("audiotranscoder%d-", langId), audioTransSeq, sep="")
  videoTransNamePool = paste(sprintf("videotranscoder%d-", langId), videoTransSeq, sep="")  # Add videotranscoder pool
  inetapNamePool = paste(sprintf("inetap%d-", langId), inetapSeq, sep="")
  
  workload_list = list()
  
  for(workloadId in 1:(maxNum))   {
    workload_list[[workloadId]] = NA
  }
    
  for(i in 1:(length(ratelist))) {
    
    stime = timelist[i]
    if( (stime < startTime) || (stime > endTime)) {
      next
    }
    stime = stime - startTime

    rate_per_vm = NUM_WORKLOAD_RATE_SCALE*ratelist[i] / maxNum
    
    for(workloadId in 1:(maxNum))   {
      seed = langId*31+i*13+(workloadId-1)*7+127

      videoName = videoNamePool[workloadId]
      audioTransName = audioTransNamePool[workloadId]
      videoTransName = videoTransNamePool[workloadId]
      inetapName = inetapNamePool[workloadId]
      
      # Generate audio transcoder workload (video -> audiotranscoder -> inetap)
      audio_workload = create_workload(durtime=interevent, n=-1,
                      seed=seed, target.rate = rate_per_vm, starttime=stime,
                      name1set=videoName, name2set=audioTransName, name3set=inetapName, 
                      filename=NA , is.dedicated=use.dedicated.network, is.append=is_append,
                      use_videotrans=FALSE)
      
      # Generate video transcoder workload (video -> videotranscoder -> inetap)
      video_workload = create_workload(durtime=interevent, n=-1,
                      seed=seed+1000, target.rate = rate_per_vm, starttime=stime,
                      name1set=videoName, name2set=videoTransName, name3set=inetapName, 
                      filename=NA , is.dedicated=use.dedicated.network, is.append=is_append,
                      use_videotrans=TRUE)
      
      # Combine both workloads into single workload list
      if(!is.na(audio_workload[1]) && !is.na(video_workload[1])) {
        # Interleave audio and video workloads
        combined_workload = rbind(audio_workload, video_workload)
        
        if(!is.list.empty(workload_list, workloadId))
          workload_list[[workloadId]] = rbind(workload_list[[workloadId]], combined_workload)
        else
          workload_list[[workloadId]] = combined_workload
      } else if(!is.na(audio_workload[1])) {
        if(!is.list.empty(workload_list, workloadId))
          workload_list[[workloadId]] = rbind(workload_list[[workloadId]], audio_workload)
        else
          workload_list[[workloadId]] = audio_workload
      } else if(!is.na(video_workload[1])) {
        if(!is.list.empty(workload_list, workloadId))
          workload_list[[workloadId]] = rbind(workload_list[[workloadId]], video_workload)
        else
          workload_list[[workloadId]] = video_workload
      }
    }
    
    # every 100 rounds, flush workload_list to a file
    if(i %% 100 == 0) {
      cat("=== Saving workload.. Start=",stime,", Target rate =",rate_per_vm, ", dur=",interevent,"\n")
      
      for(workloadId in 1:(maxNum)) {
        if(!is.list.empty(workload_list, workloadId)) {
          filename = sprintf("%d_%d_workload_wiki.csv", langId, workloadId-1)
          write.table(workload_list[[workloadId]], file=filename, row.names=FALSE, quote = FALSE, sep=",", col.names = !is_append, append = is_append)
          workload_list[[workloadId]] = NA
        }
      }
      is_append=TRUE
    }
  }
  
  for(workloadId in 1:(maxNum)) {
    if(!is.list.empty(workload_list, workloadId)) {
      filename = sprintf("%d_%d_workload_wiki.csv", langId, workloadId-1)
      write.table(workload_list[[workloadId]], file=filename, row.names=FALSE, quote = FALSE, sep=",", col.names = !is_append, append = is_append)
      workload_list[[workloadId]] = NA
    }
  }
}

main_history<-function(lang="de", startTime=0, endTime=5000) {
  history_file="overallstatpersec9-1-sfc.csv"
  historyData = read.csv(file=history_file,sep=',',header=F)

  if(lang != 0) {    
    langId=0
    fileId=0
    lang="de"
    create_historical_data(langId, timelist=historyData$V1[historyData$V2==lang],
                           ratelist=historyData$V3[historyData$V2==lang],
                           videoNum=3,
                           audioTransNum=3,
                           inetapNum=3,
                           startTime=startTime, endTime=endTime, fileId=fileId)

  }
  else {
    i=0
    for(lang in levels(historyData$V2)) {
      create_historical_data(i, timelist=histoinsinryData$V1[historyData$V2==lang],
                                ratelist=historyData$V3[historyData$V2==lang],
                             videoNum=3,
                             audioTransNum=3,
                             inetapNum=3,
                             startTime=startTime, endTime=endTime)
      i=i+1
    }
  }
}

NETWORK_PACKET_SIZE_MULTIPLY=1
CPU_MULTIPLY=1
NUM_WORKLOAD_RATE_SCALE=1

# main_history(lang='de', startTime=0, endTime=300)
main_history(lang='de', startTime=0, endTime=10000)