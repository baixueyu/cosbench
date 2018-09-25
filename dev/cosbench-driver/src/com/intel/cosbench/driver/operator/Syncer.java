package com.intel.cosbench.driver.operator;

import java.io.InputStream;
import java.util.Date;
import java.util.Random;
import java.util.*;

import org.apache.commons.io.IOUtils;

import com.intel.cosbench.api.storage.StorageException;
import com.intel.cosbench.api.storage.StorageInterruptedException;
import com.intel.cosbench.bench.Result;
import com.intel.cosbench.bench.Sample;
import com.intel.cosbench.config.Config;
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
    	Map<String, Long> syncObjs = session.getWorkContext().getMission().getObjs();
    	String srcBucketName = session.getWorkContext().getMission().getSrcBucketName();
    	String destBucketName = session.getWorkContext().getMission().getDestBucketName();
    	//TODO destBucketName exist?  begin
    	try {
    		session.getWorkContext().getDestStorageApi().createContainer(destBucketName, config);
    	} catch (Exception e) {
        	isUnauthorizedException(e, session);
        	errorStatisticsHandle(e, session, destBucketName + "/" + objectName);      
        } 
    	//TODO destBucketName exist? end







    	for (String key : syncObjs.keySet()) {
    		String objectName = key;
    		long objSize = syncObjs.get(key);
    		Sample sample = doSync(srcBucketName, destBucketName, objectName, objSize, config, session, this);
    		//TODO need to know begin
    		session.getListener().onSampleCreated(sample);
            Date now = sample.getTimestamp();
    		Result result = new Result(now, getId(), getOpType(), getSampleType(),
    				getName(), sample.isSucc());
            session.getListener().onOperationCompleted(result);
            //TODO need to know end
    	}
      	
    }
    
    public static  Sample doSync(String srcBucketName, String destBucketName, String objectName, long objSize, Config config, Session session, Operator op) {
        if (Thread.interrupted())
            throw new AbortedException();
        //TODO Get object begin 
        InputStream in = null;
        try {
        	in = session.getApi().getObject(srcBucketName, objectName, config);
        } catch (StorageInterruptedException sie) {
            doLogErr(session.getLogger(), sie.getMessage(), sie);
            throw new AbortedException();
        } catch (Exception e) {
        	isUnauthorizedException(e, session);
        	errorStatisticsHandle(e, session, destBucketName + "/" + objectName);
        	
			return new Sample(new Date(), op.getId(), op.getOpType(),
					op.getSampleType(), op.getName(), false);
			
        } 
        //TODO Get object end
        //TODO send object begin
        try {
        	session.getWorkContext().getDestStorageApi().syncObject(destBucketName, objectName, in, objSize, config);
        } catch (StorageInterruptedException sie) {
            doLogErr(session.getLogger(), sie.getMessage(), sie);
            throw new AbortedException();
        } catch (Exception e) {
        	isUnauthorizedException(e, session);
        	errorStatisticsHandle(e, session, destBucketName + "/" + objectName);
        	
			return new Sample(new Date(), op.getId(), op.getOpType(),
					op.getSampleType(), op.getName(), false);
			
        } finally {
            IOUtils.closeQuietly(in);
        }
        //TODO send object end
        
        long end = System.nanoTime();
		return new Sample(new Date(), op.getId(), op.getOpType(), op.getSampleType(),
				op.getName(), false); 
				//true, (end - start) / 1000000,
				//cin.getXferTime(), cin.getByteCount());
    }
}
