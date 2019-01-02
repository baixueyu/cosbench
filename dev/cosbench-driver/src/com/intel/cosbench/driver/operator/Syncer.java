package com.intel.cosbench.driver.operator;

import java.io.InputStream;
import java.util.*;

import org.apache.commons.io.IOUtils;

import com.intel.cosbench.api.storage.StorageException;
import com.intel.cosbench.api.storage.StorageInterruptedException;
import com.intel.cosbench.bench.Result;
import com.intel.cosbench.bench.Sample;
import com.intel.cosbench.config.Config;
import com.intel.cosbench.config.Mission;
import com.intel.cosbench.driver.generator.RandomInputStream;
import com.intel.cosbench.driver.generator.XferCountingInputStream;
import com.intel.cosbench.driver.util.ObjectPicker;
import com.intel.cosbench.driver.util.SizePicker;
import com.intel.cosbench.service.AbortedException;

public class Syncer extends AbstractOperator {

    public static final String OP_TYPE = "sync";

    private boolean chunked;
    private boolean isRandom;
    private boolean hashCheck = false;
    //private ObjectPicker objPicker = new ObjectPicker();
    //private SizePicker sizePicker = new SizePicker();
    String srcBucketName;
	String destBucketName;
	String objectName;
	long objSize;

    public Syncer() {
        /* empty */
    }

    @Override
    protected void init(String id, int ratio, String division, Config config) {
        super.init(id, ratio, division, config);
        //objPicker.init(division, config);
        //sizePicker.init(config);
        chunked = config.getBoolean("chunked", false);
        isRandom = !config.get("content", "random").equals("zero");
        hashCheck = config.getBoolean("hashCheck", false);
        
    }

    @Override
    public String getOpType() {
        return OP_TYPE;
    }
    

    public String getSrcBucketName() {
		return srcBucketName;
	}

	public void setSrcBucketName(String srcBucketName) {
		this.srcBucketName = srcBucketName;
	}

	public String getDestBucketName() {
		return destBucketName;
	}

	public void setDestBucketName(String destBucketName) {
		this.destBucketName = destBucketName;
	}

	public String getObjectName() {
		return objectName;
	}

	public void setObjectName(String objectName) {
		this.objectName = objectName;
	}

	public long getObjSize() {
		return objSize;
	}

	public void setObjSize(long objSize) {
		this.objSize = objSize;
	}

	@Override
    protected void operate(int idx, int all, Session session) {
		
		
    	Map<String, String> syncObjs = session.getWorkContext().getMission().getObjs();
    	String srcBucketName = session.getWorkContext().getMission().getSrcBucketName();
    	String destBucketName = session.getWorkContext().getMission().getDestBucketName();
    	//bandthQos�ݲ�֧��
    	//RateLimiter rateLimiter = session.getWorkContext().getRatelimiter();
    	//int bandthQos = session.getWorkContext().getBandthQos();
    	//TODO destBucketName exist?  begin
    	try {
    		session.getWorkContext().getDestStorageApi().createContainer(destBucketName, srcBucketName, session.getApi(), config);
    	} catch (Exception e) {
        	isUnauthorizedException(e, session);
        	errorStatisticsHandle(e, session, destBucketName + "/" + objectName);      
        } 
    	//TODO destBucketName exist? end
    	for (String key : syncObjs.keySet()) {
    		String objectName = key;
    		String versionId = syncObjs.get(key);
    			Sample sample = doSyncData(srcBucketName, destBucketName, objectName, versionId, config, session, this);
    		    //TODO do sync metadata begin
    		    doSyncMetaData(srcBucketName, destBucketName, objectName, config, session, this);
    		    //TODO do sync metadata end
    		    session.getListener().onSampleCreated(sample);
                Date now = sample.getTimestamp();
    		    Result result = new Result(now, getId(), getOpType(), getSampleType(), getName(), sample.isSucc());
    	//TODO destBucketName exist? end
    	for (String key : syncObjs) {
    		int index = key.indexOf("+");
    		String objectName = key.substring(0,index);
    		String versionId = key.substring(index+1, key.length());
    			//Sample sample = doSyncData(srcBucketName, destBucketName, objectName, versionId, config, session, this, rateLimiter, bandthQos);
    			Sample sample = doSyncData(srcBucketName, destBucketName, objectName, versionId, config, session, this, null, 0);
    		    //TODO do sync metadata begin
    		    doSyncMetaData(srcBucketName, destBucketName, objectName, config, session, this);
    		    //TODO do sync metadata end
    		    session.getListener().onSampleCreated(sample);
                Date now = sample.getTimestamp();
    		    Result result = new Result(now, getId(), getOpType(), getSampleType(), getName(), sample.isSucc());
                session.getListener().onOperationCompleted(result);	
              //TODO need to know end
        	} 
    }
    
    private void doSyncMetaData(String srcBucketName, String destBucketName,
			String objectName, Config config, Session session,
			Syncer syncer) {
		// TODO Auto-generated method stub
    	//TODO sync ACL begin
    	//TODO sync ACL end
    	
    	//TODO sync user metadata begin 
    	//TODO sync user metadata end
	public static  Sample doSyncData(String srcBucketName, String destBucketName, String objectName, String versionId, Config config, Session session, Operator op, RateLimiter rateLimiter, int bandthQos) {
		if (Thread.interrupted())
			throw new AbortedException();
        //TODO Get object begin 
        InputStream in = null;
        List<Long> objSize = new ArrayList<Long>(1);
   //   int syncObjFailLimit = session.getWorkContext().getMission().getSyncObjFailLimit();
   //   System.out.println("syncObjFailLimit:"+syncObjFailLimit);
        try {
        	in = session.getApi().getObject(srcBucketName, objectName, versionId, objSize, config);
        } catch (StorageInterruptedException sie) {
            doLogErr(session.getLogger(), sie.getMessage(), sie);
            throw new AbortedException();
        } catch (Exception e) {
        	syncException(e, session);
        	errorStatisticsHandle(e, session, destBucketName + "/" + objectName);
			return new Sample(new Date(), op.getId(), op.getOpType(),
					op.getSampleType(), op.getName(), false);		
        } 
        //TODO Get object end
        //TODO send object begin
        long start = System.nanoTime();
        XferCountingInputStream cin = new XferCountingInputStream(in);
        boolean succ = true;
        try {
        	List<String> upload_id = new ArrayList<String>(1); 
            List<Object> partETags = new ArrayList<Object>();
            long lastSyncStartTime = session.getWorkContext().getMission().getLastSyncStartTime();
            System.out.println("get last start time:" + lastSyncStartTime);
        	int i = 0;
        	boolean need = session.getWorkContext().getDestStorageApi().needSyncOrNot(destBucketName, srcBucketName, objectName, lastSyncStartTime, session.getApi(), versionId);
        	if(!need){
        		System.out.println(objectName + "ÒÑ´æÔÚÓÚÄ¿µÄÍ°£¬±¾´ÎÍ¬²½Ìø¹ý");
        		doLogInfo(session.getLogger(), objectName + "ÒÑ´æÔÚÓÚÄ¿µÄÍ°£¬±¾´ÎÍ¬²½Ìø¹ý");
        	} else {
        		do {
        			int result = session.getWorkContext().getDestStorageApi().syncObject(destBucketName,srcBucketName, objectName, cin, objSize.get(0), upload_id, partETags, versionId, session.getApi(), config, rateLimiter, null);
        			System.out.println(objectName + "���ϴ�" + (i + 1) + "��");
        			if (result == 0) {
        				System.out.println(objectName + "µÚ" + (i + 1) + "´ÎÉÏ´«ºó³É¹¦");
        				doLogInfo(session.getLogger(), objectName + "µÚ" + (i + 1) + "´ÎÉÏ´«ºó³É¹¦");
        				break;
        			} else {
        				IOUtils.closeQuietly(cin);
        				in = session.getApi().getObject(srcBucketName, objectName, versionId, objSize, config);
        				cin = new XferCountingInputStream(in);
        				i++;
        				System.out.println(objectName + "µÚ" + i + "´ÎÉÏ´«ºóÊ§°Ü");
        			}		
        		} while (i < 5);
        	}
				op.getName(), true, (end - start) / 1000000, (end - start) / 1000000, objSize.get(0));
    }
}
