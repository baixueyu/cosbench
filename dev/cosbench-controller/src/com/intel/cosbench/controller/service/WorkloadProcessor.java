/** 
 
Copyright 2013 Intel Corporation, All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. 
*/ 

package com.intel.cosbench.controller.service;

import java.util.*;

import static com.intel.cosbench.model.WorkloadState.CANCELLED;
import static com.intel.cosbench.model.WorkloadState.FAILED;
import static com.intel.cosbench.model.WorkloadState.FINISHED;
import static com.intel.cosbench.model.WorkloadState.PROCESSING;
import static com.intel.cosbench.model.WorkloadState.SUSPEND;
import static com.intel.cosbench.model.WorkloadState.QUEUING;
import static com.intel.cosbench.model.WorkloadState.TERMINATED;
import static com.intel.cosbench.model.WorkloadState.isStopped;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.inspur.ratelimit.RateLimiter;
import com.inspur.ratelimit.RateLimiterFactory;
import com.inspur.ratelimit.RedisUtil;
import com.intel.cosbench.api.S3Stor.S3Storage;
import com.intel.cosbench.config.Config;
import com.intel.cosbench.config.Operation;
import com.intel.cosbench.config.Stage;
import com.intel.cosbench.config.Work;
import com.intel.cosbench.config.WorkloadResolver;
import com.intel.cosbench.config.XmlConfig;
import com.intel.cosbench.config.castor.CastorConfigTools;
import com.intel.cosbench.config.common.KVConfigParser;
import com.intel.cosbench.controller.model.ControllerContext;
import com.intel.cosbench.controller.model.DriverContext;
import com.intel.cosbench.controller.model.DriverRegistry;
import com.intel.cosbench.controller.model.StageContext;
import com.intel.cosbench.controller.model.StageRegistry;
import com.intel.cosbench.controller.model.TaskContext;
import com.intel.cosbench.controller.model.TaskRegistry;
import com.intel.cosbench.controller.model.WorkloadContext;
import com.intel.cosbench.log.LogFactory;
import com.intel.cosbench.log.Logger;
import com.intel.cosbench.model.StageState;
import com.intel.cosbench.service.CancelledException;
import com.intel.cosbench.service.IllegalStateException;

/**
 * This class encapsulates workload processing logic.
 * 
 * @author ywang19, qzheng7
 * 
 */
class WorkloadProcessor {

    private static final Logger LOGGER = LogFactory.getSystemLogger();

    private WorkloadContext workloadContext;
    private ControllerContext controllerContext;

    private ExecutorService executor;
    private List<StageContext> queue;

    public WorkloadProcessor() {
        /* empty */
    }

    public WorkloadContext getWorkloadContext() {
        return workloadContext;
    }

    public void setWorkloadContext(WorkloadContext workloadContext) {
        this.workloadContext = workloadContext;
        /*should be set after controllerContext set*/
        this.workloadContext.setDriverRegistry(controllerContext.getDriverRegistry());
    }

    public void setControllerContext(ControllerContext controllerContext) {
        this.controllerContext = controllerContext;
    }

    public void init() {
        resolveWorklaod();
        createStages();
        createExecutor();
    }

    public void dispose() {
	    if (executor != null)
	        executor.shutdown();
	    executor = null;
	}

	private void resolveWorklaod() {
        XmlConfig config = workloadContext.getConfig();
        WorkloadResolver resolver = CastorConfigTools.getWorkloadResolver();
        workloadContext.setWorkload(resolver.toWorkload(config));
    }

    private void createStages() {
        StageRegistry registry = new StageRegistry();
        int index = 1;
        for (Stage stage : workloadContext.getWorkload().getWorkflow()) {
            String id = "s" + index++ + "-" + stage.getName();
            registry.addStage(createStageContext(id, stage));
        }
        workloadContext.setStageRegistry(registry);
    }

    private static StageContext createStageContext(String id, Stage stage) {
    	initStageOpId(stage);
        StageContext context = new StageContext();
        context.setId(id);
        context.setStage(stage);
        context.setState(StageState.WAITING);
        return context;
    }
    
    private static void initStageOpId(Stage stage) {
    	int index = 0;
		for (Work work : stage.getWorks()) {
			for (Operation op : work.getOperations())
				op.setId("op" + String.valueOf(++index));
		}
    }

    private void createExecutor() {
        executor = Executors.newFixedThreadPool(2);
        StageRegistry registry = workloadContext.getStageRegistry();
        queue = new LinkedList<StageContext>(registry.getAllItems());
    }

    public void process() {
        /* for strong consistency: a lock should be employed here */
        if (!workloadContext.getState().equals(QUEUING))
            throw new IllegalStateException(
                    "workload should be in the state of queuing but " + workloadContext.getState().name());
        String id = workloadContext.getId();
        LOGGER.info("begin to process workload {}", id);
        long startTime;
        long timeConsuming;
        try {
        	workloadContext.setStartDate(new Date());
        outerLoop:	while (true) {
            				startTime = System.currentTimeMillis();
            				System.out.println("本次："+startTime);
            	           	System.out.println("上次："+workloadContext.getLastSyncStartTime());
        					processWorkload();
        					workloadContext.setLastSyncStartTime(startTime);
        					workloadContext.setState(SUSPEND);
        					timeConsuming = System.currentTimeMillis() - startTime;
            				workloadContext.setTimeConsuming(millisToHMS(timeConsuming));
        					while(true){
        						if (workloadContext.getIncremental()!=null && workloadContext.getIncremental().length()!=0) {
        							if (workloadContext.getIncremental().equals("true")) {
        								
        								System.out.println("jixu...");
        								break;
        							} 
        							if(workloadContext.getIncremental().equals("false")){
        								System.out.println("tuichu...");
        								break outerLoop;
        							} 
        						} else {
        							try{
        								Thread.sleep(5000);
        							} catch (Exception e){
        								System.exit(0);
        							}
        						}
        					}
        					workloadContext.setIncremental(null);
        					workloadContext.setTimeConsuming(null);
        					startTime = 0;
        					timeConsuming = 0;	
        			}
        	workloadContext.setStopDate(new Date());
        	workloadContext.setState(FINISHED);
        } catch (CancelledException ce) {
            cancelWorkload();
            return;
        } catch (WorkloadException we) {
            terminateWorkload();
            return;
		} catch (InterruptedException e) {
			terminateWorkload();
			return;
		} catch (Exception e) {
            LOGGER.error("unexpected exception", e);
            terminateWorkload();
            return;
        }
        workloadContext.logErrorStatistics(LOGGER);
        LOGGER.info("sucessfully processed workload {}", id);
    }
    
    private String configurationSyncMerge(){
    	StringBuilder str = new StringBuilder();
    	str.append(controllerContext.getBucket_policy());
    	str.append(controllerContext.getBucket_lifecycle_configuration());
    	str.append(controllerContext.getBucket_cross_origin_configuration());
    	str.append(controllerContext.getBucket_website_configuration());
    	return str.toString();
    }

    /*
     * There is a small window when the workload is 'PROCESSING' but there is no
     * 'current stage' set! However, this inconsistent window is left AS-IS for
     * performance consideration.
     */
    private void processWorkload() throws InterruptedException {
        workloadContext.setState(PROCESSING);
      //  workloadContext.setStartDate(new Date());
        Iterator<StageContext> iter = queue.iterator();
        String trigger = workloadContext.getWorkload().getTrigger();
        executeTrigger(trigger, true, workloadContext.getId());
        while (iter.hasNext()) {
            StageContext stageContext = iter.next();
            //String marker = new String();   
            if (stageContext.getStage().getName().equals("sync")) {
            	List<Work> works = stageContext.getStage().getWorks();
            	for (Work work : works) {
            		work.getSync().setConfigurationSync(configurationSyncMerge());
            		work.getSync().setLastSyncStartTime(workloadContext.getLastSyncStartTime());
            		String syncStr = work.getConfig();
        			Config syncConfig = KVConfigParser.parse(syncStr);
        			String storageConfig = work.getSync().getSyncStorage().getConfig();
        		//	String qosConfig = work.getSync().getQos().getConfig();
        			int iopsQos = 0;
        			RateLimiter iopsLimiter = null;
        			getQosParmas(iopsQos, iopsLimiter, syncConfig, work);
        			int syncNum;
        			try {
        				syncNum = syncConfig.getInt("syncNum");
        				if (syncNum <= 0) {
        					syncNum = 1000;
        				}
        			} catch (Exception e) {
        				syncNum = 1000;
        			}
            		if (syncConfig.get("sync_type").equals("bucket")) {
            			bucketSync(syncConfig, storageConfig, work, stageContext, syncNum, iopsQos, iopsLimiter);             
            		} else if (syncConfig.get("sync_type").equals("user")) {
            			userSync(storageConfig, work, stageContext, syncNum, iopsQos, iopsLimiter);	                 			         			     	               	 	               	
                	}
            	}
            //	iter.remove();
            } else {
            	 iter.remove();
                 runStage(stageContext);
            }          
        }
        executeTrigger(trigger, false, workloadContext.getId());
     //   workloadContext.setStopDate(new Date());
        workloadContext.setCurrentStage(null);
        workloadContext.mergeErrorStatistics();
		for (StageContext stageContext : workloadContext.getStageRegistry()
				.getAllItems()) {
			if (stageContext.getState().equals(StageState.FAILED)) {
				workloadContext.setState(FAILED);
				return;
			}
		}
   //     workloadContext.setState(FINISHED);
    }

	private void userSync(String storageConfig, Work work,
			StageContext stageContext, int syncNum, int iopsQos,
			RateLimiter iopsLimiter) throws InterruptedException {
		// TODO Auto-generated method stub
		Config config = getSrcStorageConfig(storageConfig);
		List<String> buckets = new ArrayList<String>();
		buckets = getSrcBuckets(config);   
		Map<String, String> nextMarker = new HashMap<String, String>(1);             			        				
		for (String bucketName : buckets) {
			String srcBucket = bucketName;
			String destBucket = bucketName;
			while (true) {                    	
				config = getSrcStorageConfig(storageConfig);
				setSyncInfo(config, srcBucket, destBucket, nextMarker, work, stageContext, syncNum, iopsQos, iopsLimiter);         
				runStage(stageContext);
				String key = null;
				String versionIdMarker = null;
				for (String keyMarker : nextMarker.keySet()) {
					versionIdMarker = nextMarker.get(keyMarker);
					key = keyMarker;
				}
				if ((key == null || key.length() <= 0) && (versionIdMarker == null || versionIdMarker.length() <= 0)) {
					break;
				}
			}
		}     
	}

	private void bucketSync(Config syncConfig, String storageConfig, Work work,
			StageContext stageContext, int syncNum, int iopsQos,
			RateLimiter iopsLimiter) throws InterruptedException {
		// TODO Auto-generated method stub
		String srcBucket = syncConfig.get("srcBucket");
   	 	String destBucket = syncConfig.get("destBucket"); 
   		if (destBucket.isEmpty()) {
   	 		destBucket = srcBucket;
   	 	}
   		
   		Map<String, String> nextMarker = new HashMap<String, String>(1);
   	 	while (true) {                    	
       	 	Config config = getSrcStorageConfig(storageConfig);
       	 	setSyncInfo(config, srcBucket, destBucket, nextMarker, work, stageContext, syncNum, iopsQos, iopsLimiter);         
			runStage(stageContext);
       		String key = null;
			String versionIdMarker = null;
			for (String keyMarker : nextMarker.keySet()) {
				key = keyMarker;
				versionIdMarker = nextMarker.get(keyMarker);
			}
			if ((key == null || key.length() <= 0) && (versionIdMarker == null || versionIdMarker.length() <= 0)) {
				break;
			}
   	 	}     	
	}

	private void getQosParmas(int iopsQos, RateLimiter iopsLimiter, Config syncConfig, Work work) {
		// TODO Auto-generated method stub
		RedisUtil redis = null;
		iopsQos = getIopsQosValue(syncConfig);
		String bandthQos = getBandthQosValue(syncConfig);
		redis = new RedisUtil("10.180.210.55", 6379, "1q2w3e4r!");
		RateLimiterFactory rateLimiterFactory = new RateLimiterFactory();
		if (iopsQos != 0) {
			iopsLimiter = rateLimiterFactory.build("ratelimiter:iops",
					(double)iopsQos, 30, redis);
		} 
		//bandthQos在controller只进行设置并初始化，不需要返回
		if (bandthQos != null) {
			double bandth = Double.valueOf(bandthQos.substring(0, bandthQos.length() - 3));
			RateLimiter bandthLimiter = rateLimiterFactory.build("ratelimiter:bandth",
					bandth, 30, redis);
		}
	}

	private int getIopsQosValue(Config syncConfig) {
		// TODO Auto-generated method stub
		int iops = 0;
		try {
			String iopsStr = syncConfig.get("iopsQos");
			if (iopsStr.charAt(iopsStr.length() -1) != 'n') {
				return 0;
			}
			iops =Integer.parseInt(iopsStr.substring(0, iopsStr.length() - 1));
		} catch (Exception e) {
            return 0;
		}
		return iops;
	}

	private String getBandthQosValue(Config syncConfig) {
		// TODO Auto-generated method stub
		String bandth = null;
		try {
			String bandthStr = syncConfig.get("bandthQos");
			int length = bandthStr.length();
			if (bandthStr.charAt(length - 4) != 'n' && bandthStr.charAt(length - 3) != ':' && 
					bandthStr.charAt(length - 1) != 'k' && bandthStr.charAt(length - 1) != 'K' &&
					bandthStr.charAt(length - 1) != 'M' && bandthStr.charAt(length - 1) != 'm') {
				return null;
			}
			bandth = bandthStr.substring(0, bandthStr.length() - 4) + bandthStr.substring(length - 3 , length);
		} catch (Exception e) {
            return null;
		}
		return bandth;
	}

	private Config getSrcStorageConfig(String storageConfig) {
    	// TODO Auto-generated method stub
    	Config workCon =  KVConfigParser.parse(storageConfig);
		 
    	String accesskey = workCon.get("srcAccessKey");
    	String entry = "accesskey=" + accesskey + ";";
    	String secretkey = workCon.get("srcSecretKey");
    	entry = entry + "secretkey=" + secretkey + ";";
    	String endpoint = workCon.get("syncFrom");
    	entry = entry + "endpoint=" + endpoint + ";";
    	return KVConfigParser.parse(entry);
	}

	private List<String> getSrcBuckets(Config srcStorageConfig) {
		// TODO Auto-generated method stub			 
		S3Storage s3Storage = new S3Storage();
		s3Storage.init(srcStorageConfig, LOGGER);
		return s3Storage.listBuckets();
	}

	private void setSyncInfo(Config srcStorageConfig, String srcBucket, String destBucket, Map<String, String> marker, Work work, StageContext stageContext, int syncNum, int iopsQos, RateLimiter iopsLimiter){		
		 List<List<String>> objsList = new ArrayList<List<String>>(); 
		 int drivers = controllerContext.getDriverCount();
		 S3Storage s3Storage = new S3Storage();
		 s3Storage.init(srcStorageConfig, LOGGER);
		 //String nextMarker;
		 while (true) {
			 try {
				 if (iopsLimiter.tryAcquire(iopsQos, 1, TimeUnit.SECONDS)) {
					 for (int i=0; i< drivers; i++) {
						 List<String> objs = new ArrayList<String>();
						 s3Storage.listVersions(srcBucket, marker, objs, syncNum);
						 objsList.add(objs);
						 //解决循环list不能停止在问题
						 for(String keyMarker : marker.keySet()) {
							 String versionIdMarker = marker.get(keyMarker);
							 if ((keyMarker == null || keyMarker.length() <= 0) && (versionIdMarker == null || versionIdMarker.length() <= 0)) {
								 break;
							 }
							 marker.put(keyMarker, versionIdMarker);
						 }
						 if (marker == null || marker.size() < 1) {
							 break;
						 }
					 } 
					 if (objsList.size() < drivers) {
						 for (int i = 0; i < drivers - objsList.size(); i++) {
							 objsList.add(null);
						 }
					 }	
					 break;
				 } else {
					 continue;
				 }
			 } catch (InterruptedException e) {
				 // TODO Auto-generated catch block
				 e.printStackTrace();
			 }
		 }
		 stageContext.setObjsList(objsList);
         work.getSync().setSrcBucketName(srcBucket);
         work.getSync().setDestBucketName(destBucket);
   }

    private static String millisToHMS(long millis) {

        long hrs = MILLISECONDS.toHours(millis) % 24;
        long min = MILLISECONDS.toMinutes(millis) % 60;
        long sec = MILLISECONDS.toSeconds(millis) % 60;

        return hrs + ":" + min + "::" + sec;
    }

    private void runStage(StageContext stageContext) throws InterruptedException {
        String id = stageContext.getId();
        int closuredelay = stageContext.getStage().getClosuredelay();

        String stageName = stageContext.getStage().getName();
//        String work0Type = stageContext.getStage().getWorks().get(0).getType();

        LOGGER.info("begin to run stage {}", id);

        LOGGER.info("============================================");
        LOGGER.info("START WORK: {}", stageName);

        long startStamp = System.currentTimeMillis();

        workloadContext.setCurrentStage(stageContext);
        if (stageName.equals("delay") && closuredelay > 0) {
			executeDelay(stageContext, closuredelay);
		} else {
			executeStage(stageContext);

			long elapsedTime = System.currentTimeMillis() - startStamp;

			LOGGER.info("END WORK:   {}, Time elapsed: {}", stageName, millisToHMS(elapsedTime));
			LOGGER.info("============================================");
			if(closuredelay > 0)
				executeDelay(stageContext, closuredelay);
		}
		LOGGER.info("successfully ran stage {}", id);
		
		List<String> killDrivers = new ArrayList<String>();
		killDrivers = stageContext.getKillDriver();
		DriverRegistry registryOld = new DriverRegistry();
		DriverRegistry registryNew = new DriverRegistry();
		registryOld = controllerContext.getDriverRegistry();
		DriverContext[] driverContexts = registryOld.getAllDrivers();
		if (killDrivers!=null && !killDrivers.isEmpty()) {
			for(int i=0; i<driverContexts.length; i++){
				if (!killDrivers.contains(driverContexts[i].getName())) {
					registryNew.addDriver(driverContexts[i]);
				}
		    }
		//	DriverContext[] test = registryNew.getAllDrivers();
		    controllerContext.setDriverRegistry(registryNew);
		} 
	}
    
	private void executeDelay(StageContext stageContext, int closuredelay)
			throws InterruptedException {

		LOGGER.info("sleeping for " + closuredelay + " seconds...");
		stageContext.setState(StageState.SLEEPING);
		Thread.sleep(closuredelay * 1000);
		LOGGER.info("sleep complete.");
		stageContext.setState(StageState.COMPLETED);
	} 

    private void executeStage(StageContext stageContext) {
        StageRunner runner = createStageRunner(stageContext);
        StageChecker checker = createStageChecker(stageContext);
        StageCallable[] callables = new StageCallable[] { runner, checker };
        String wsId = workloadContext.getId()+stageContext.getId();
        String trigger = stageContext.getStage().getTrigger();
        executeTrigger(trigger, true, wsId);
        try {
            executor.invokeAll(Arrays.asList(callables));
        } catch (InterruptedException ie) {
        	executeTrigger(trigger, false, wsId);
            throw new CancelledException(); // workload cancelled
        }
        runner.dispose(); // early dispose runner
        executeTrigger(trigger, false, wsId);
        if (!stageContext.getState().equals(StageState.TERMINATED))
            return;
        String id = stageContext.getId();
        LOGGER.error("detected stage {} encountered error", id);
        throw new WorkloadException(); // mark termination
    }

    private StageRunner createStageRunner(StageContext stageContext) {
        StageRunner runner = new StageRunner();
        runner.setStageContext(stageContext);
        runner.setControllerContext(controllerContext);
        runner.init();
        return runner;
    }

    private StageChecker createStageChecker(StageContext stageContext) {
        StageChecker checker = new StageChecker();
        checker.setStageContext(stageContext);
        return checker;
    }

    private void terminateWorkload() {
        String id = workloadContext.getId();
        LOGGER.info("begin to terminate workload {}", id);
        for (StageContext stageContext : queue)
            stageContext.setState(StageState.ABORTED);
        executeTrigger(workloadContext.getWorkload().getTrigger(), false, workloadContext.getId());
        workloadContext.setStopDate(new Date());
        workloadContext.setState(TERMINATED);
        LOGGER.info("successfully terminated workload {}", id);
    }
    
    private void executeTrigger(String trigger, boolean isEnable, String wsId) {
    	if (trigger == null || trigger.isEmpty())
			return;
    	TriggerRunner runner = new TriggerRunner(workloadContext.getDriverRegistry());
		runner.runTrigger(isEnable, trigger, wsId);
	}

    public void cancel() {
        String id = workloadContext.getId();
        Future<?> future = workloadContext.getFuture();
        /* for strong consistency: a lock should be employed here */
        if (future != null) {
            if (future.isCancelled())
                return; // already cancelled
            if (future.cancel(true)) {
                if (workloadContext.getState().equals(QUEUING)) {
                    for (StageContext stageContext : queue)
                        stageContext.setState(StageState.CANCELLED);
                    workloadContext.setStopDate(new Date());
                    workloadContext.setState(CANCELLED); // cancel it directly
                    LOGGER.info("successfully cancelled workload {}", id);
                    return; // workload cancel before processing
                }
                return; // cancel request submitted
            }
        }
        if (isStopped(workloadContext.getState())) {
            LOGGER.warn("workload {} not aborted as it is already stopped", id);
            return; // do nothing -- it is already stopped
        }
        workloadContext.setStopDate(new Date());
        workloadContext.setState(CANCELLED); // cancel it directly
        LOGGER.info("successfully cancelled workload {}", id);
    }

    private void cancelWorkload() {
        String id = workloadContext.getId();
        LOGGER.info("begin to cancel workload {}", id);
        executor.shutdown();
        if (Thread.interrupted())
            LOGGER.warn("get cancelled when canceling workload {}", id);
        try {
        	if (!executor.awaitTermination(5, TimeUnit.SECONDS)
        			&& !executor.awaitTermination(5, TimeUnit.SECONDS))
				executor.shutdownNow();
            if (!awaitTermination(5) && !awaitTermination(10) && !awaitTermination(30))
            	LOGGER.warn("get cancelled when canceling workload {}", id);
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        if (!executor.isTerminated())
            LOGGER.warn("fail to cancel current stage for workload {}", id);
        /*
         * Consider the workload aborted even if its current stage has not.
         */
        for (StageContext stageContext : queue)
            stageContext.setState(StageState.CANCELLED);
        executeTrigger(workloadContext.getWorkload().getTrigger(), false, workloadContext.getId());
        workloadContext.setStopDate(new Date());
        workloadContext.setState(CANCELLED);
        LOGGER.info("successfully cancelled workload {}", id);
    }
    
    private boolean awaitTermination(int seconds) {
        try {
            if (!executor.isTerminated()) {
                LOGGER.info("wait {} seconds for workload to cancel ...", seconds);
                executor.awaitTermination(seconds, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            LOGGER.debug("get cancelled when canceling workload");
        }
        return executor.isTerminated();
    }

}
