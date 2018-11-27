package com.intel.cosbench.config;

import java.util.List;
import java.util.Map;


public class Sync {
	private Storage storage;   
	private Qos qos;
    private String srcBucketName;
    private String destBucketName;
    private List<String> objs;
    
	public Storage getSyncStorage() {
		return storage;
	}
	public void setSyncStorage(Storage storage) {
		this.storage = storage;
	}
	
	public Qos getQos() {
		return qos;
	}
	public void setQos(Qos qos) {
		this.qos = qos;
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
	public List<String> getObjs() {
		return objs;
	}
	public void setObjs(List<String> objs) {
		this.objs = objs;
	}

	
}
