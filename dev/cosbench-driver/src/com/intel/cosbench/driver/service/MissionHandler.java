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

package com.intel.cosbench.driver.service;

import static com.intel.cosbench.model.MissionState.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

//import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;

import com.intel.cosbench.api.auth.*;
import com.intel.cosbench.api.context.AuthContext;
import com.intel.cosbench.api.storage.*;
import com.intel.cosbench.config.*;
import com.intel.cosbench.config.castor.CastorConfigTools;
import com.intel.cosbench.config.common.KVConfigParser;
import com.intel.cosbench.driver.agent.*;
import com.intel.cosbench.driver.model.*;
import com.intel.cosbench.driver.operator.Operators;
import com.intel.cosbench.driver.util.OperationPicker;
import com.intel.cosbench.log.*;
import com.intel.cosbench.service.*;
import com.intel.cosbench.service.IllegalStateException;
import com.intel.cosbench.service.TimeoutException;

class MissionHandler {

    private static final String AUTH_RETRY_KEY = "retry";

    private static final int DEFAULT_AUTH_RETRY = 0;

    private static final Logger LOGGER = LogFactory.getSystemLogger();

    private static final File LOG_DIR = new File(new File("log"), "mission");
    
    private String type;

    static {
        if (!LOG_DIR.exists())
            LOG_DIR.mkdirs();
    }

    private int retry; // auth retry number
    private Config authConfig; // shared auth configurations
    private Config storageConfig; // shared storage configurations

    private AuthAPIService authAPIs;
    private StorageAPIService storageAPIs;

    private ExecutorService executor;
    private MissionContext missionContext;

    public MissionHandler() {
        /* empty */
    }

    public MissionContext getMissionContext() {
        return missionContext;
    }

    public void setMissionContext(MissionContext missionContext) {
        this.missionContext = missionContext;
    }

    public void setAuthAPIs(AuthAPIService authAPIs) {
        this.authAPIs = authAPIs;
    }

    public void setStorageAPIs(StorageAPIService storageAPIs) {
        this.storageAPIs = storageAPIs;
    }
    
    public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void dispose() {
        if (executor != null)
            executor.shutdown();
        executor = null;
    }

    public void init() {
        resolveMission();
        openLogger();
        createOperators();
        initOpPicker();
        parseConfigs();
        createWorkers();
        createExecutor();
    }

    private void resolveMission() {
        XmlConfig config = missionContext.getConfig();
        MissionResolver resolver = CastorConfigTools.getMissionResolver();
        missionContext.setMission(resolver.toMission(config));
    }

    private void openLogger() {
        LogManager manager = LogFactory.createLogManager();
        String name = missionContext.getId() + ".log";
        try {
            boolean append = false;
            boolean buffer = true;
            manager.setLogFile(LOG_DIR, name, append, buffer);
        } catch (IOException e) {
            LOGGER.error("cannot open log file", e);
        }
        LogManager sysManager = LogFactory.getSystemLogManager();
        manager.setLogLevel(sysManager.getLogLevel());
        missionContext.setLogManager(manager);
    }

    private void createOperators() {
        OperatorRegistry registry = new OperatorRegistry();
        Mission mission = missionContext.getMission();
        initOpDefaultName(mission);
        for (Operation op : mission) {
        	if (op.getType().equals("sync")) {
        		setType("sync");
        	}
            registry.addOperator(createOperatorContext(op));
        }
        missionContext.setOperatorRegistry(registry);
    }

    private static OperatorContext createOperatorContext(Operation op) {
        OperatorContext context = new OperatorContext();
        Config config = KVConfigParser.parse(op.getConfig());
        context.setOperator(Operators.getOperator(op, config));
        return context;
    }
    
	private void initOpDefaultName(Mission mission) {
		Set<String> opTypes = new HashSet<String>();
		for (Operation op : mission.getOperations()) {
			opTypes.add(op.getType());
		}
		if (opTypes.size() == mission.getOperations().size())
			return;
		for (String opType : opTypes) {
			int index = 0;
			for (Operation op : mission.getOperations()) {
				if (op.getType().equals(opType)) {
					index++;
					if (op.getConfig().indexOf("name") < 0) {
						op.setConfig(StringUtils.join(new Object[] {
								op.getConfig(), ";name=",
								String.valueOf(index), "-", op.getType() }));
					}
				}
			}
		}
	}

    private void initOpPicker() {
        OperationPicker picker = new OperationPicker();
        Mission mission = missionContext.getMission();
        for (Operation op : mission)
        	picker.addOperation(op.getId(), op.getRatio());
        missionContext.setOperationPicker(picker);
    }

    private void parseConfigs() {
        Mission m = missionContext.getMission();
        authConfig = KVConfigParser.parse(m.getAuth().getConfig());
        retry = authConfig.getInt(AUTH_RETRY_KEY, DEFAULT_AUTH_RETRY);
        storageConfig = KVConfigParser.parse(m.getStorage().getConfig());
        LOGGER.debug("driver mission config  is: "+m.getConfig());
    }

    private void createWorkers() {
        WorkerRegistry registry = new WorkerRegistry();
        Mission mission = missionContext.getMission();
        int workers = mission.getWorkers();
        int offset = mission.getOffset();
        if (getType() != null &&  getType().equals("sync")) {
        	//TODO for sync operator set object_list to registry, divide equally
        	boolean isEmpty = false;
        	List<String> objs = mission.getObjs();
        	if (objs == null || objs.size() == 0) {
        		isEmpty = true;
        	}
        	//TODO just for test
        	//Map<String, Long> objs = new HashMap<String, Long>();
        	//objs.put("obj1", (long) 111);
        	//objs.put("obj2", (long) 222);
        	//objs.put("obj3", (long) 333);
        	//objs.put("obj4", (long) 444);
        	int objSize = 0;
        	if (!isEmpty) {
        		objSize = objs.size();   
        	}       	     	
        	int listSize  = objSize / workers;
        	//every workers can deal one objs sync
        	if (objSize <= workers) {
        		listSize = 1;
        	}     	
        	int count = objSize / listSize;   
            int yu = objSize % listSize;   
       
            for (int i = 0; i <= count && !isEmpty; i++) {
            	
            	Mission taskMission = new Mission(mission);
            	List<String> syncObjs = new ArrayList<String>(); 
                if (i == count) {   
                	int sub = i * listSize;
                	Iterator<String> iter = objs.iterator();
                	while (iter.hasNext()) {
                		if (sub == (i * listSize + yu)) {
                			break;
                		}
                		String key = iter.next();
                		syncObjs.add(key); 
                		iter.remove();
                		sub++;
                	}                       
                } else {   
                	int sub = i * listSize;
                	Iterator<String> iter = objs.iterator();
                	while (iter.hasNext()) {
                		if (sub == listSize * (i + 1)) {
                			break;
                		}
                		String key = iter.next();
                		syncObjs.add(key); 
                		iter.remove();
                		sub++;
                	}  
                }
                
                // Get sync subMap
                if (syncObjs.size() == 0) {
                	break;
                }
                taskMission.setObjs(syncObjs);
              
                
                //set srcBucketName & destBucketName
                //mission.setSrcBucketName(msrcBucketName);
                //mission.setDestBucketName(destBucketName);
                registry.addWorker(createWorkerContext(i + offset + 1, taskMission));
                
            }
        } else {
            for (int idx = 1; idx <= workers; idx++) {
            	registry.addWorker(createWorkerContext(idx + offset, mission));
            }
        }        
        missionContext.setWorkerRegistry(registry);
        
    }

    private WorkerContext createWorkerContext(int idx, Mission mission) {
    	
    	
    	LogManager manager = missionContext.getLogManager();
        WorkerContext context = new WorkerContext();
        context.setIndex(idx);
        context.setMission(mission);
        context.setLogger(manager.getLogger());
        context.setErrorStatistics(missionContext.getErrorStatistics());
        context.setAuthApi(createAuthApi(mission.getAuth(), manager));
        if(getType() != null && getType().equals("sync")) {        	
        	context.setStorageApi(createSyncStorageApi(mission.getStorage(), manager, "src"));
        	context.setDestStorageApi(createSyncStorageApi(mission.getStorage(), manager, "dest"));
        } else {
        	context.setStorageApi(createStorageApi(mission.getStorage(), manager));
        }
        return context;
    }

	private AuthAPI createAuthApi(Auth auth, LogManager manager) {
        String type = auth.getType();
        Logger logger = manager.getLogger();
        return authAPIs.getAuth(type, authConfig, logger);
    }

    private StorageAPI createStorageApi(Storage storage, LogManager manager) {
        String type = storage.getType();
        Logger logger = manager.getLogger();
        return storageAPIs.getStorage(type, storageConfig, logger);
    }
    
    private StorageAPI createSyncStorageApi(Storage storage, LogManager manager, String storageType) {
    	Config syncStorageConfig = null;
    	String accesskey;
    	String secretkey;
    	String endpoint;
    	String entry;
    	String type = storage.getType();
        Logger logger = manager.getLogger();
    	if (storageType.equals("src")) {
    		accesskey = storageConfig.get("srcAccessKey");
    		entry = "accesskey=" + accesskey + ";";
    		secretkey = storageConfig.get("srcSecretKey");
    		entry = entry + "secretkey=" + secretkey + ";";
    		endpoint = storageConfig.get("syncFrom");
    		entry = entry + "endpoint=" + endpoint + ";";
    		syncStorageConfig = KVConfigParser.parse(entry);
    	} else if (storageType.equals("dest")) {
    		accesskey = storageConfig.get("destAccessKey");
    		entry = "accesskey=" + accesskey + ";";
    		secretkey = storageConfig.get("destSecretKey");
    		entry = entry + "secretkey=" + secretkey + ";";
    		endpoint = storageConfig.get("destTo");
    		entry = entry + "endpoint=" + endpoint + ";";
    		syncStorageConfig = KVConfigParser.parse(entry);
    	}
        return storageAPIs.getStorage(type, syncStorageConfig, logger);
    }

    private void createExecutor() {
        Mission mission = missionContext.getMission();
        int workers = mission.getWorkers();
        executor = Executors.newFixedThreadPool(workers);
    }

    public void login() {
        /* for strong consistency: a lock should be employed here */
        if (!missionContext.getState().equals(SUBMITTED))
            throw new IllegalStateException(
                    "mission should be in the state of submitted but " + missionContext.getState().name());
        String id = missionContext.getId();
        LOGGER.debug("begin to auth mission {}", id);
        try {
            performLogin();
        } catch (AbortedException ae) {
            abortAgents(true);
            missionContext.setState(ABORTED);
            return;
        } catch (MissionException me) {
            missionContext.setState(TERMINATED);
            LOGGER.info("mission {} has been terminated", id);
            return;
        } catch (Exception e) {
            missionContext.setState(TERMINATED);
            LOGGER.error("unexpected exception", e);
            LOGGER.info("mission {} has been terminated", id);
            return;
        }
        LOGGER.info("mission {} has been authed successfully", id);
    }

    private void performLogin() {
        missionContext.setState(AUTHING);

//        // Use worker 0 for authentication (and use the same token for other workers)
//        WorkerContext worker0 = missionContext.getWorkerRegistry().getWorkerByIndex(0);
//        List<Agent> agents = createAuthAgentFromContext(worker0);
        List<Agent> agents = createAuthAgents();
        executeAgents(agents, 0);

//        AuthContext authContext = worker0.getStorageApi().getAuthContext();
//        setAllWorkersAuthContext(authContext);

        missionContext.setState(AUTHED);
    }

    @SuppressWarnings("unused")
    private void setAllWorkersAuthContext(AuthContext authContext) {
	for (WorkerContext workerContext : missionContext.getWorkerRegistry())
            workerContext.getStorageApi().setAuthContext(authContext);
    }

    /***
     * Returns Size 1 list of Agents
     * @param workerContext to create Agent for
     */
    @SuppressWarnings("unused")
    private List<Agent> createAuthAgentFromContext(WorkerContext workerContext) {
        List<Agent> agents = new ArrayList<Agent>();
        agents.add(Agents.newAuthAgent(retry, workerContext));
        return agents;
    }

	private List<Agent> createAuthAgents() {
        List<Agent> agents = new ArrayList<Agent>();
        for (WorkerContext workerContext : missionContext.getWorkerRegistry())
            agents.add(Agents.newAuthAgent(retry, workerContext));
        return agents;
    }

    public void stress() {
        /* for strong consistency: a lock should be employed here */
        if (!missionContext.getState().equals(AUTHED))
            throw new IllegalStateException(
                    "mission should be in the state of authed");
        String id = missionContext.getId();
        LOGGER.debug("begin to execute mission {}", id);
        try {
        	Mission.setSyncObjFailCount(0);
            stressTarget();
        } catch (TimeoutException te) {
            /* no need to shutdown agents again */
            boolean shutdownNow = false;
            abortAgents(shutdownNow);
//            missionContext.setState(ABORTED);
            return;
        } catch (AbortedException ae) {
            /* have to shutdown agents now */
            boolean shutdownNow = true;
            abortAgents(shutdownNow);
            missionContext.setState(ABORTED);
            return;
        } catch (MissionException me) {
            missionContext.setState(TERMINATED);
            LOGGER.info("mission {} has been terminated", id);
            System.out.println(missionContext.getState()+"missionHandler");
            return;
        } catch (Exception e) {
            missionContext.setState(TERMINATED);
            LOGGER.error("unexpected exception", e);
            LOGGER.info("mission {} has been terminated", id);
            return;
        }
        LOGGER.info("mission {} has been executed successfully", id);
       
    }

    private void stressTarget() {
        missionContext.setState(LAUNCHED);
        List<Agent> agents = createWorkAgents();
        Mission m = missionContext.getMission();
        int timeout = m.getRampup() + m.getRuntime() + m.getRampdown();
        executeAgents(agents, timeout == 0 ? 0 : timeout + 60);
        missionContext.setState(FINISHED);
        missionContext.getErrorStatistics().summaryToMission(missionContext.getLogManager().getLogger());
    }

    private List<Agent> createWorkAgents() {
        List<Agent> agents = new ArrayList<Agent>();
        for (WorkerContext workerConext : missionContext.getWorkerRegistry())
            agents.add(Agents.newWorkAgent(workerConext, missionContext));
        return agents;
    }

    private void executeAgents(List<Agent> agents, int timeout) {	
        int num = agents.size();
        LOGGER.debug("begin to execute agents, {} in total", num);
        try {
            if (timeout == 0){
            	List<Future<Agent>> futures = executor.invokeAll(agents); // wait until finish
                for(Future<Agent> future : futures){
                	if(future.get() != null){
                	    future.get();
                	}
                	
                }
            }
            else {
                List<Future<Agent>> futures = executor.invokeAll(agents,
                        timeout, TimeUnit.SECONDS);
                for (Future<Agent> future : futures){
                	 if (future.isCancelled()){ // test timeout status
                        throw new TimeoutException(); // force mission abort
                	 }
                }   
            }
        } catch (InterruptedException ie) {
            throw new AbortedException(); // mission aborted
        } catch (ExecutionException e) {
			e.printStackTrace();
			if(e.getCause().toString().contains("AbortedException")){
				missionContext.getMission().setState("abort");
			}
		} 
        LOGGER.debug("all {} agents have finished execution", num);
        List<Integer> errIds = new ArrayList<Integer>();
        for (WorkerContext worker : missionContext.getWorkerRegistry())
            if (worker.isError() || worker.isAborted())
                errIds.add(worker.getIndex());
        if (errIds.isEmpty())
            return; // all of the workers are fine
        LOGGER.error("detected workers {} have encountered errors", errIds);
        throw new MissionException(); // mark termination
    }

    public void close() {
        /* for strong consistency: a lock should be employed here */
        if (!missionContext.getState().equals(FINISHED))
            throw new IllegalStateException(
                    "mission should be in the state of finished");
        String id = missionContext.getId();
        missionContext.setState(ACCOMPLISHED);
		for (int i = 0; i < missionContext.getReport().getAllMetrics().length; i++) {
			LOGGER.debug("!!!! mission op: "
					+ missionContext.getReport().getAllMetrics()[i].getOpType()
					+ "-"
					+ missionContext.getReport().getAllMetrics()[i].getOpType());
			if (missionContext.getReport().getAllMetrics()[i].getSampleCount() == 0
					&& missionContext.getReport().getAllMetrics()[i]
							.getTotalSampleCount() > 0) {
				missionContext.setState(FAILED);
				LOGGER.debug("!!!! mission opt -> FAILED");
				break;
			}
		}
        LOGGER.info("mission {} has been closed successfully", id);
    }

    public void abort() {
        String id = missionContext.getId();
        Future<?> future = missionContext.getFuture();
        /* for strong consistency: a lock should be employed here */
        if (future != null) {
            if (future.isCancelled())
                return; // already aborted
            if (future.cancel(true))
                return; // abort request submitted
        }
        if (isStopped(missionContext.getState())) {
            LOGGER.warn("mission {} not aborted as it is already stopped", id);
            return; // do nothing -- it is already stopped
        }
        missionContext.setState(ABORTED); // abort it directly
        LOGGER.info("mission {} has been aborted successfully", id);
    }

    private void abortAgents(boolean shutdownNow) {
        Thread.interrupted(); // clear interruption status
        
    	executor.shutdown(); 
    	try {     
    		// Wait a few seconds for existing tasks to terminate    
    		if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {       
    			executor.shutdownNow();
    			
    	        String id = missionContext.getId();
    			
    	        if (!awaitTermination(5) && !awaitTermination(10) && !awaitTermination(30)) 
    				LOGGER.warn("fail to abort agents for mission {}", id);
    			else
    				LOGGER.info("all agents have been aborted in mission {}", id);

    	        LOGGER.info("mission {} appears to be aborted", id); // agents aborted
    		}   

    	} catch (InterruptedException ie) {     
    			executor.shutdownNow();     
    			Thread.currentThread().interrupt();   
    	} 
    }

    private boolean awaitTermination(int seconds) {
        try {
            if (!executor.isTerminated()) {
                LOGGER.info("wait {} seconds for agents to abort ...", seconds);
                executor.awaitTermination(seconds, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            LOGGER.debug("get aborted when aborting mission");
        }
        return executor.isTerminated();
    }

}
