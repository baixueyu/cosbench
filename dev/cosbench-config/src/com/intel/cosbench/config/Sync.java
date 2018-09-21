package com.intel.cosbench.config;

import java.util.Map;


public class Sync {
	private Storage storage;    
    private String srcBucketName;
    private String destBucketName;
    private Map<String,Long> objs;
    
	public Storage getSyncStorage() {
		return storage;
	}
	public void setSyncStorage(Storage storage) {
		this.storage = storage;
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
	public Map<String, Long> getObjs() {
		return objs;
	}
	public void setObjs(Map<String, Long> objs) {
		this.objs = objs;
	}
	
}
