package com.github.kafka.monitor.util;

import java.io.IOException;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class HttpUtil {
	
	private static HttpClient httpClient = null;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtil.class);
	
	public static String sendPostRequest(String body,String url, Map<String,String> headerMap, boolean isGzip) {
		String content = null;
		//LOGGER.info("Sending Post Request to URL : {} with Body : {} ",url,body);
		if(httpClient==null) {
			httpClient =  HttpClientBuilder.create().build();
		}
		HttpPost httpPost = new HttpPost(url);
		try {
			for (Map.Entry<String, String> header : headerMap.entrySet()) {
				httpPost.addHeader(header.getKey(),header.getValue());
			}
			StringEntity stringEntity = new StringEntity(body);
			if(isGzip) {
				GzipCompressingEntity gzipEntity = new GzipCompressingEntity(stringEntity);
				httpPost.setEntity(gzipEntity);
			}else {
				httpPost.setEntity(stringEntity);
			}
			HttpResponse response = httpClient.execute(httpPost);
			HttpEntity respEntity = response.getEntity();
			if (respEntity != null) {
				 content = EntityUtils.toString(respEntity);
			}
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			httpPost = null;
			//LOGGER.info("Compelted HTTP Post request call with response : {}",content);
		}
		return content;
	}
}
