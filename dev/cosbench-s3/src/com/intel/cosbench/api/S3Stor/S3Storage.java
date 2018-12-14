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
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpStatus;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PartSummary;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.model.VersionListing;
import com.inspur.ratelimit.RateLimiter;
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

		boolean pathStyleAccess = config.getBoolean(PATH_STYLE_ACCESS_KEY,
				PATH_STYLE_ACCESS_DEFAULT);

		String proxyHost = config.get(PROXY_HOST_KEY, "");
		String proxyPort = config.get(PROXY_PORT_KEY, "");

		parms.put(ENDPOINT_KEY, endpoint);
		parms.put(AUTH_USERNAME_KEY, accessKey);
		parms.put(AUTH_PASSWORD_KEY, secretKey);
		parms.put(PATH_STYLE_ACCESS_KEY, pathStyleAccess);
		parms.put(PROXY_HOST_KEY, proxyHost);
		parms.put(PROXY_PORT_KEY, proxyPort);

		logger.debug("using storage config: {}", parms);
		// SkipMd5CheckStrategy.DISABLE_GET_OBJECT_MD5_VALIDATION_PROPERTY =
		// false;
		ClientConfiguration clientConf = new ClientConfiguration();
		clientConf.setConnectionTimeout(timeout);
		clientConf.setSocketTimeout(timeout);
		clientConf.withUseExpectContinue(false);
		clientConf.withSignerOverride("S3SignerType");
		clientConf.setMaxConnections(500);
		clientConf.setMaxErrorRetry(10);
		// clientConf.setProtocol(Protocol.HTTP);
		if ((!proxyHost.equals("")) && (!proxyPort.equals(""))) {
			clientConf.setProxyHost(proxyHost);
			clientConf.setProxyPort(Integer.parseInt(proxyPort));
		}

		AWSCredentials myCredentials = new BasicAWSCredentials(accessKey,
				secretKey);
		client = new AmazonS3Client(myCredentials, clientConf);
		client.setEndpoint(endpoint);
		client.setS3ClientOptions(new S3ClientOptions()
				.withPathStyleAccess(pathStyleAccess));

		logger.debug("S3 client has been initialized");
	}

	public AmazonS3 getClient() {
		return client;
	}

	@Override
	public void setAuthContext(AuthContext info) {
		super.setAuthContext(info);
		// try {
		// client = (AmazonS3)info.get(S3CLIENT_KEY);
		// logger.debug("s3client=" + client);
		// } catch (Exception e) {
		// throw new StorageException(e);
		// }
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
	public InputStream getObject(String container, String object,
			String versionId, List<Long> size, Config config) {
		super.getObject(container, object, config);
		InputStream stream;
		try {

			S3Object s3Obj = client.getObject(new GetObjectRequest(container,
					object, versionId));
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
			if (!client.doesBucketExist(container)) {

				client.createBucket(container);
			}
		} catch (Exception e) {
			throw new StorageException(e);
		}
	}

	@Override
	public void createContainer(String container, String srcContainer,
			StorageAPI srcS3Storage, Config config) {
		try {
			S3Storage s3 = (S3Storage) srcS3Storage;
			AmazonS3 srcClient = s3.getClient();
			BucketVersioningConfiguration srcVersionConfig = srcClient
					.getBucketVersioningConfiguration(srcContainer);

			if (!client.doesBucketExist(container)) {
				client.createBucket(container);
				// TODO ACL should be considerd
				// S3Storage s3 = (S3Storage) srcS3Storage;
				// AmazonS3 srcClient = s3.getClient();
				// srcClient.getBucketAcl(container);
				// client.setBucketAcl(container,
				// srcClient.getBucketAcl(container));
			}
			if (srcVersionConfig.getStatus().equals("Off")
					&& !client.getBucketVersioningConfiguration(container)
							.getStatus().equals("Off")) {
				srcVersionConfig.setStatus("Suspended");
				client.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(
						container, srcVersionConfig));
			}
		   if (srcVersionConfig.getStatus().equals("Enabled")
					&& !client.getBucketVersioningConfiguration(container)
					.getStatus().equals("Enabled")) {
				srcVersionConfig.setMfaDeleteEnabled(null);
			    client.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(
						container, srcVersionConfig));
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
		// super.createObject(container, object, data, length, config);
		try {
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(length);
			metadata.setContentType("application/octet-stream");
			PutObjectRequest request = new PutObjectRequest(container, object,
					data, metadata);
			request.getRequestClientOptions().setReadLimit(15728640); // 15MB
			client.putObject(request);
		} catch (Exception e) {
			throw new StorageException(e);
		}
	}

	@Override
	public void deleteContainer(String container, Config config) {
		super.deleteContainer(container, config);
		try {
			if (client.doesBucketExist(container)) {
				client.deleteBucket(container);
			}
		} catch (AmazonS3Exception awse) {
			if (awse.getStatusCode() != HttpStatus.SC_NOT_FOUND) {
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
		} catch (AmazonS3Exception awse) {
			if (awse.getStatusCode() != HttpStatus.SC_NOT_FOUND) {
				throw new StorageException(awse);
			}
		} catch (Exception e) {
			throw new StorageException(e);
		}
	}

	public List<String> listBuckets() {
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

	public String listObjects(String bucketName, String marker,
			Map<String, Long> objs, int num) {
		try {
			// HashMap<String,Long> objs = new HashMap<String, Long>();
			ListObjectsRequest req = new ListObjectsRequest(bucketName, null,
					marker, null, num);
			ObjectListing ol = client.listObjects(req);
			marker = ol.getNextMarker();
			List<S3ObjectSummary> objects = ol.getObjectSummaries();
			for (S3ObjectSummary os : objects) {
				objs.put(os.getKey(), os.getSize());
				// os.getKey();
			}
			// client.listObjects(bucketName);
			return marker;
		} catch (Exception e) {
			throw new StorageException(e);
		}
	}

	public void listVersions(String bucketName, Map<String, String> marker,
			List<String> objs, int num) {
		try {
			// HashMap<String,Long> objs = new HashMap<String, Long>();
			String versionIdMarker = null;
			String keyMarker = null;
			// 遍历删除，只保留一个
			Iterator<Map.Entry<String, String>> it = marker.entrySet()
					.iterator();
			while (it.hasNext()) {
				Map.Entry<String, String> entry = it.next();
				keyMarker = entry.getKey();
				versionIdMarker = entry.getValue();
				it.remove();
			}
			VersionListing vl = client.listVersions(bucketName, null,
					keyMarker, versionIdMarker, null, num);
			// reset keyMarker & versionIdMarker
			keyMarker = vl.getNextKeyMarker();
			versionIdMarker = vl.getNextVersionIdMarker();
			if (keyMarker != null && keyMarker.length() != 0) {
				marker.put(keyMarker, versionIdMarker);
			}
			// Get the result obj
			List<S3VersionSummary> vs = vl.getVersionSummaries();
			for (S3VersionSummary iter : vs) {
			//	objs.put(iter.getKey(), iter.getVersionId());
				objs.add(iter.getKey() + "+" + iter.getVersionId());
			}
			// client.listObjects(bucketName);
		} catch (Exception e) {
			throw new StorageException(e);
		}
	}

	/**
	 * 同步前判断
	 * 获取上次同步的起始时间及源桶中对象的上次修改时间
	 * 若该对象的上次修改时间<上次同步的起始时间，则说明该对象在上次同步期间未被修改，不再同步
	 * 对于上次同步期间未被修改又未同步成功的对象，存储到本地，最后单独处理
	 * 若对象的上次修改时间>=上次同步的起始时间，则说明上次同步期间肯定被修改了，则无需做其他判断，直接同步即可
	 * @return true 需要同步，  false 不需要同步
	 */
	@Override
	public boolean needSyncOrNot(String container, String srcContainer, String object, long lastSyncStartTime, StorageAPI srcS3Storage, String versionId){		
		S3Storage s3 = (S3Storage) srcS3Storage;
		AmazonS3 srcClient = s3.getClient();
		ObjectMetadata metadata = srcClient.getObjectMetadata(new GetObjectMetadataRequest(srcContainer, object, versionId));
		Date lastModiDate = metadata.getLastModified();
		long lastModiMills = lastModiDate.getTime(); 
		System.out.println("get last modi time Date:" + object + " " + lastModiDate);
		System.out.println("get last modi time millsec:" + object + " " + lastModiMills);
		 
		
		//如果上次同步起始时间为0，说明是第一次同步，则对所有对象执行同步
		if (lastSyncStartTime == 0) {
			return true;
		 }
		else {
		    if (lastModiMills < lastSyncStartTime) {
		    	return false;
		    }
		    else {
		    	return true;
		    } 
		}
	}
	
	
	@Override
	public int syncObject(String container, String srcContainer, String object, InputStream data, long content_length, List<String> upload_id,
			List<Object> partETags, String versionId, StorageAPI srcS3Storage, Config config, RateLimiter ratelimiter) {
		// super.createObject(container, object, data, length, config);
		super.syncObject(container, srcContainer, object, data, content_length, upload_id, partETags, versionId, srcS3Storage, config, ratelimiter);
		long part_size = 15 * 1024 * 1024;
		int success = 0;;
		try {
			if (content_length < part_size) {
				S3Storage s3 = (S3Storage) srcS3Storage;
				AmazonS3 srcClient = s3.getClient();
				// srcClient.getObjectAcl(srcContainer, object);
				ObjectMetadata metadata = srcClient.getObjectMetadata(new GetObjectMetadataRequest(srcContainer, object, versionId));
				PutObjectRequest request = new PutObjectRequest(container, object, data, metadata);
				request.getRequestClientOptions().setReadLimit(15728640); // 15MB
				client.putObject(request);
				// TODO ACL need to consider
				// AccessControlList acl = srcClient.getObjectAcl(srcContainer,
				// object);
				// client.setObjectAcl(container, object, versionId, acl);
			} else {
				success = syncMultipartObject(container, data, object, content_length, upload_id, partETags, part_size, srcS3Storage, srcContainer, versionId);
			}
		} catch (Exception e) {
			success --;
			e.printStackTrace();
		}
		return success;
	}
	
	/**
	 * 多片上传主函数
	 * @param bucket_name 目的桶名
	 * @param in 所上传的对象的数据流
	 * @param key_name 所上传的对象名
	 * @param content_length 所上传的对象的大小
	 * @param upload_id 多片上传的标识，用于断点续传
	 * @param partETags 已成功上传的对象分片信息，用于断点续传
	 * @param part_size 分片大小
	 * @param srcS3Storage
	 * @param srcContainer 源桶名
	 * @param versionId
	 * @return
	 */
	public int syncMultipartObject(String bucket_name, InputStream in, String key_name, long content_length, List<String> upload_id,
			List<Object> partETags, long part_size, StorageAPI srcS3Storage, String srcContainer, String versionId) {
		List<PartSummary> remote_parts = new ArrayList<PartSummary>();
		try {
			if (null == upload_id || upload_id.size() <= 0) {
				init_multipart_upload(bucket_name, key_name, upload_id, srcS3Storage, srcContainer, versionId);
			} else {
				remote_parts = get_parts_information(bucket_name, key_name, upload_id);
			}
			upload_all_part(bucket_name, in, key_name, content_length, upload_id, remote_parts, partETags, part_size);
			complete_multipart_upload(bucket_name, key_name, upload_id, partETags);
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
		return 0;
	}

	/**
	 * 获取已成功上传至集群的分片信息
	 * @param bucket_name
	 * @param key_name
	 * @param upload_id
	 * @return
	 */
	public List<PartSummary> get_parts_information(String bucket_name, String key_name, List<String> upload_id) {
		ListPartsRequest part_list = new ListPartsRequest(bucket_name, key_name, upload_id.get(0));
		PartListing list = client.listParts(part_list);
		List<PartSummary> parts = list.getParts();
		System.out.println("重传，故本次不需要初始化");
		return parts;
	}

	/**
	 * 多片上传第一阶段：初始化多片上传
	 * @param bucket_name
	 * @param key_name
	 * @param upload_id
	 * @param srcS3Storage
	 * @param srcContainer
	 * @param versionId
	 * @return
	 */
	public int init_multipart_upload(String bucket_name, String key_name, List<String> upload_id, StorageAPI srcS3Storage, String srcContainer, String versionId) {
		InitiateMultipartUploadRequest init_request = new InitiateMultipartUploadRequest(bucket_name, key_name);
		S3Storage s3 = (S3Storage) srcS3Storage;
		AmazonS3 srcClient = s3.getClient();
		GetObjectMetadataRequest getObjectMetadataRequest = new GetObjectMetadataRequest(srcContainer, key_name, versionId);
		ObjectMetadata metadata = srcClient.getObjectMetadata(getObjectMetadataRequest);
		init_request.setObjectMetadata(metadata);
		InitiateMultipartUploadResult init_response = client.initiateMultipartUpload(init_request);
		upload_id.add(init_response.getUploadId());
		System.out.println("init multipart ...done");
		return 0;
	}

	/**
	 * 多片上传第二阶段：循环上传所有分片
	 * @param bucket_name
	 * @param in
	 * @param key_name
	 * @param content_length
	 * @param upload_id
	 * @param flag
	 * @param partETags
	 * @param part_size
	 * @return
	 */
	public int upload_all_part(String bucket_name, InputStream in, String key_name, long content_length, List<String> upload_id,
			List<PartSummary> remote_parts, List<Object> partETags, long part_size) {
		int seq = 0;
		long file_position = 0;
		for ( ; file_position < content_length; ) {
			part_size = Math.min(part_size, content_length - file_position);
			upload_part(bucket_name, in, key_name, part_size, seq, remote_parts, upload_id, partETags);
			file_position += part_size;
			seq += 1;
		}
		return 0;
	}

	/**
	 * 上传每个分片，对于已经成功上传至集群的分片则直接跳过
	 * @param bucket_name
	 * @param in
	 * @param key_name
	 * @param part_size
	 * @param seq
	 * @param remote_parts
	 * @param upload_id
	 * @param partETags
	 * @return
	 */
	public int upload_part(String bucket_name, InputStream in, String key_name, long part_size, int seq, List<PartSummary> remote_parts,
			List<String> upload_id, List<Object> partETags) {
		PartSummary remote_part = null;
		if (remote_parts.size() > seq) {
			remote_part = remote_parts.get(seq);
		} else {
			remote_part = null;
		}
		if (null != remote_part) {
			if (remote_part.getSize() == part_size) {
				PartETag partETag = (PartETag) partETags.get(seq);
				if (remote_part.getETag().equals(partETag.getETag())) {
					System.out.println("size and etag match for part " + (seq + 1) + ":skipping");
					return 0;
				} else {
					System.out.println("etag does not match");
				}
			} else {
				System.out.println("size does not match");
			}
		}
		UploadPartRequest upload_request = new UploadPartRequest()
				.withBucketName(bucket_name)
				.withKey(key_name)
				.withUploadId(upload_id.get(0))
				.withPartNumber(seq + 1)
				.withInputStream(in)
				.withPartSize(part_size);
		upload_request.getRequestClientOptions().setReadLimit(15728640); // 15MB
		UploadPartResult upload_response = client.uploadPart(upload_request);
		partETags.add(upload_response.getPartETag());
		System.out.println(key_name + "第" + (seq + 1) + "片上传成功");
		return 0;
	}

	/**
	 * 多片上传第三阶段：完成分片上传
	 * @param bucket_name
	 * @param key_name
	 * @param upload_id
	 * @param partETags
	 * @return
	 */
	public int complete_multipart_upload(String bucket_name, String key_name, List<String> upload_id, List<Object> partETags) {
		List<PartETag> partETags_correct = new ArrayList<PartETag>();
		for (int i = 0; i < partETags.size(); i++) {
			partETags_correct.add((PartETag) partETags.get(i));
		}
		CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucket_name, key_name, upload_id.get(0), partETags_correct);
		client.completeMultipartUpload(compRequest);
		System.out.println("complete multipart upload...done");
		return 0;
	}

}
