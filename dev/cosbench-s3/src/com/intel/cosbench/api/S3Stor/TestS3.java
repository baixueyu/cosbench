package com.intel.cosbench.api.S3Stor;

import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class TestS3 {
	 public static void main(String[] args)
	    {
		 	AWSCredentials myCredentials = new BasicAWSCredentials("k11", "s11");
		 	ClientConfiguration clientConf = new ClientConfiguration();
		 	//clientConf.setProxyHost("http://10.180.210.41");
			//clientConf.setProxyPort(Integer.parseInt("8009"));
		 	clientConf.withSignerOverride("S3SignerType");
		 	clientConf.setProtocol(Protocol.HTTP);
		 	clientConf.setConnectionTimeout(30000);
	        clientConf.setSocketTimeout(30000);
	        clientConf.withUseExpectContinue(false);
	        AmazonS3 s3 = new AmazonS3Client(myCredentials, clientConf);
	        s3.setEndpoint("http://10.180.210.41:8009");
	        
	        /*
	        List<Bucket> buckets = s3.listBuckets();

	        //s3.l
	        System.out.println("Your Amazon S3 buckets are:");
	        System.out.println(buckets.size());
	        
	        for (Bucket b : buckets) {
	            System.out.println("* " + b.getName());
	        }
	        */
	        ObjectMetadata om= s3.getObjectMetadata("bucket1","35M");
	        Long len = om.getContentLength();
	        System.out.println(len+"------");
	        ListObjectsRequest req = new ListObjectsRequest("bucket1", null, "", null, 2); 
	        ObjectListing ol = s3.listObjects(req);
	        List<S3ObjectSummary>  objects = ol.getObjectSummaries();
	        for (S3ObjectSummary os : objects) {
				System.out.println("*"+os.getKey()+"-----"+os.getSize());
				//System.out.println(os.getSize());
				//os.getSize();
			}
	        System.out.println(ol.getNextMarker());
			
	    }
}
