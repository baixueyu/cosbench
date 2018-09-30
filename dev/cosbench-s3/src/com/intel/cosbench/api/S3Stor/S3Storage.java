package com.intel.cosbench.api.S3Stor;

import static com.intel.cosbench.client.S3Stor.S3Constants.AUTH_PASSWORD_DEFAULT;
import static com.intel.cosbench.client.S3Stor.S3Constants.AUTH_PASSWORD_KEY;
import static com.intel.cosbench.client.S3Stor.S3Constants.AUTH_USERNAME_DEFAULT;
import static com.intel.cosbench.client.S3Stor.S3Constants.AUTH_USERNAME_KEY;
import static com.intel.cosbench.client.S3Stor.S3Constants.CONN_TIMEOUT_DEFAULT;
import static com.intel.cosbench.client.S3Stor.S3Constants.CONN_TIMEOUT_KEY;
import static com.intel.cosbench.client.S3Stor.S3Constants.ENDPOINT_DEFAULT;
import static com.intel.cosbench.client.S3Stor.S3Constants.ENDPOINT_KEY;
import static com.intel.cosbench.client.S3Stor.S3Constants.PATH_STYLE_ACCESS_DEFAULT;
import static com.intel.cosbench.client.S3Stor.S3Constants.PATH_STYLE_ACCESS_KEY;
import static com.intel.cosbench.client.S3Stor.S3Constants.PROXY_HOST_KEY;
import static com.intel.cosbench.client.S3Stor.S3Constants.PROXY_PORT_KEY;


import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpStatus;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListBucketsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.s3.internal.SkipMd5CheckStrategy;
import com.intel.cosbench.api.context.AuthContext;
import com.intel.cosbench.api.storage.NoneStorage;
import com.intel.cosbench.api.storage.StorageAPI;
import com.intel.cosbench.api.storage.StorageException;
import com.intel.cosbench.config.Config;
import com.intel.cosbench.log.Logger;

public class S3Storage extends NoneStorage {
	private int timeout;
	
    private String accessKey;
    private String secretKey;
    private String endpoint;
    
    private AmazonS3 client;

    @Override
    public void init(Config config, Logger logger) {
    	super.init(config, logger);
    	
    	timeout = config.getInt(CONN_TIMEOUT_KEY, CONN_TIMEOUT_DEFAULT);

    	parms.put(CONN_TIMEOUT_KEY, timeout);
    	
    	endpoint = config.get(ENDPOINT_KEY, ENDPOINT_DEFAULT);
        accessKey = config.get(AUTH_USERNAME_KEY, AUTH_USERNAME_DEFAULT);
        secretKey = config.get(AUTH_PASSWORD_KEY, AUTH_PASSWORD_DEFAULT);

        boolean pathStyleAccess = config.getBoolean(PATH_STYLE_ACCESS_KEY, PATH_STYLE_ACCESS_DEFAULT);
        
		String proxyHost = config.get(PROXY_HOST_KEY, "");
		String proxyPort = config.get(PROXY_PORT_KEY, "");
        
        parms.put(ENDPOINT_KEY, endpoint);
    	parms.put(AUTH_USERNAME_KEY, accessKey);
    	parms.put(AUTH_PASSWORD_KEY, secretKey);
    	parms.put(PATH_STYLE_ACCESS_KEY, pathStyleAccess);
    	parms.put(PROXY_HOST_KEY, proxyHost);
    	parms.put(PROXY_PORT_KEY, proxyPort);

        logger.debug("using storage config: {}", parms);
        //SkipMd5CheckStrategy.DISABLE_GET_OBJECT_MD5_VALIDATION_PROPERTY = false;
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setConnectionTimeout(timeout);
        clientConf.setSocketTimeout(timeout);
        clientConf.withUseExpectContinue(false);
        clientConf.withSignerOverride("S3SignerType");
//        clientConf.setProtocol(Protocol.HTTP);
		if((!proxyHost.equals(""))&&(!proxyPort.equals(""))){
			clientConf.setProxyHost(proxyHost);
			clientConf.setProxyPort(Integer.parseInt(proxyPort));
		}
        
        AWSCredentials myCredentials = new BasicAWSCredentials(accessKey, secretKey);
        client = new AmazonS3Client(myCredentials, clientConf);
        client.setEndpoint(endpoint);
        client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(pathStyleAccess));
        
        logger.debug("S3 client has been initialized");
    }
    
    public AmazonS3 getClient() {
		return client;
	}

	@Override
    public void setAuthContext(AuthContext info) {
        super.setAuthContext(info);
//        try {
//        	client = (AmazonS3)info.get(S3CLIENT_KEY);
//            logger.debug("s3client=" + client);
//        } catch (Exception e) {
//            throw new StorageException(e);
//        }
    }

    @Override
    public void dispose() {
        super.dispose();
        client = null;
    }

	@Override
    public InputStream getObject(String container, String object, Config config) {
        super.getObject(container, object, config);
        InputStream stream;
        try {
        	
            S3Object s3Obj = client.getObject(container, object);
            stream = s3Obj.getObjectContent();
            
        } catch (Exception e) {
            throw new StorageException(e);
        }
        return stream;
    }
	
	@Override
	public InputStream getObject(String container, String object, String versionId, List<Long> size, Config config) {
	        super.getObject(container, object, config);
	        InputStream stream;
	        try {
	        	
	            S3Object s3Obj = client.getObject(new GetObjectRequest(container, object, versionId));
	            size.add(s3Obj.getObjectMetadata().getContentLength());
	            stream = s3Obj.getObjectContent();
	            
	        } catch (Exception e) {
	            throw new StorageException(e);
	        }
	        return stream;
	    }

    @Override
    public void createContainer(String container, Config config) {
        super.createContainer(container, config);
        try {
        	if(!client.doesBucketExist(container)) {
	        	
	            client.createBucket(container);
        	}
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    @Override
    public void createContainer(String container, StorageAPI  srcS3Storage, Config config) {
        
        try {
        	if(!client.doesBucketExist(container)) {
	        	
	            client.createBucket(container);            
	            S3Storage s3 = (S3Storage) srcS3Storage;
	            AmazonS3 srcClient = s3.getClient();	            
	            //srcClient.getBucketAcl(container);
	            client.setBucketAcl(container, srcClient.getBucketAcl(container));
	           
        	}
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

	@Override
    public void createObject(String container, String object, InputStream data,
            long length, Config config) {
        super.createObject(container, object, data, length, config);
        try {
    		ObjectMetadata metadata = new ObjectMetadata();
    		metadata.setContentLength(length);
    		metadata.setContentType("application/octet-stream");
    		
        	client.putObject(container, object, data, metadata);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
	
	@Override
    public void syncObject(String container, String object, InputStream data,
            long length, Config config) {    	
        //super.createObject(container, object, data, length, config);
        try {
    		ObjectMetadata metadata = new ObjectMetadata();
    		metadata.setContentLength(length);
    		metadata.setContentType("application/octet-stream");
    		PutObjectRequest request = new PutObjectRequest(container, object, data, metadata);
    		request.getRequestClientOptions().setReadLimit(15728640); //15MB
        	client.putObject(request);       
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    public void syncMultipartObject(String container, String object, InputStream data,
            long length, Config config) {
        try {
        	//TODO for MultipartUpload
    		ObjectMetadata metadata = new ObjectMetadata();
    		metadata.setContentLength(length);
    		metadata.setContentType("application/octet-stream");
    		PutObjectRequest request =new PutObjectRequest(container, object, data, metadata);
    		request.getRequestClientOptions().setReadLimit(15728640); //15MB
        	client.putObject(request);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void deleteContainer(String container, Config config) {
        super.deleteContainer(container, config);
        try {
        	if(client.doesBucketExist(container)) {
        		client.deleteBucket(container);
        	}
        } catch(AmazonS3Exception awse) {
        	if(awse.getStatusCode() != HttpStatus.SC_NOT_FOUND) {
        		throw new StorageException(awse);
        	}
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void deleteObject(String container, String object, Config config) {
        super.deleteObject(container, object, config);
        try {
            client.deleteObject(container, object);
        } catch(AmazonS3Exception awse) {
        	if(awse.getStatusCode() != HttpStatus.SC_NOT_FOUND) {
        		throw new StorageException(awse);
        	}
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    public List<String> listBuckets(){   
    	try {
    		List<String> buckets = new ArrayList<String>();
    		List<Bucket> listBucket = client.listBuckets();
    		for (Bucket bucket : listBucket) {
    			buckets.add(bucket.getName());
			}
    		return buckets;
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    public String listObjects(String bucketName, String marker , Map<String, Long> objs, int num){
    	try {
    		 //HashMap<String,Long> objs = new  HashMap<String, Long>();
    		 ListObjectsRequest req = new ListObjectsRequest(bucketName, null, marker, null, num);
    		 ObjectListing ol = client.listObjects(req);
    		 marker = ol.getNextMarker();
    		 List<S3ObjectSummary>  objects = ol.getObjectSummaries();
    		 for (S3ObjectSummary os : objects) {
    			objs.put(os.getKey(), os.getSize());
				//os.getKey();
			}
   		 	//client.listObjects(bucketName);
    		 return marker;
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
   
    public void syncObject(String container, String srcContainer, String object, InputStream data,
            String versionId, StorageAPI  srcS3Storage, Config config) {    	
        //super.createObject(container, object, data, length, config);
        try {
        	S3Storage s3 = (S3Storage) srcS3Storage;
            AmazonS3 srcClient = s3.getClient();
            //srcClient.getObjectAcl(srcContainer, object);
            ObjectMetadata metadata = srcClient.getObjectMetadata(new GetObjectMetadataRequest(srcContainer, object, versionId));
           
    		PutObjectRequest request = new PutObjectRequest(container, object, data, metadata);
    		request.getRequestClientOptions().setReadLimit(15728640); //15MB
        	client.putObject(request);    
        	AccessControlList acl = srcClient.getObjectAcl(srcContainer, object);
        	client.setObjectAcl(container, object, versionId, acl);
            
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    public String listVersions(String bucketName, String marker , Map<String, String> objs, int num){
    	try {
    		 //HashMap<String,Long> objs = new  HashMap<String, Long>();
    		VersionListing vl = client.listVersions(bucketName, null, marker, null, null, num);
    		 
    		 marker = vl.getNextKeyMarker();
    		 List<S3VersionSummary> vs= vl.getVersionSummaries();
    		 for (S3VersionSummary iter : vs) {
    			 objs.put(iter.getKey(), iter.getVersionId());
				;
			}
   		 	//client.listObjects(bucketName);
    		 return marker;
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

}
