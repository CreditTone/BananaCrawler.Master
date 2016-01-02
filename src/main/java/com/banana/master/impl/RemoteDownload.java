package com.banana.master.impl;

import java.rmi.RemoteException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.banana.common.download.IDownload;
import com.banana.request.BasicRequest;
import com.banana.request.StartContext;

public class RemoteDownload {
	
	private static Logger logger = Logger.getLogger(RemoteDownload.class);
	
	private LoadBalance loadBalance = new LoadBalance();
	
	private Map<String,StartContext> contextCache = new ConcurrentHashMap<String,StartContext>();
	
	public RemoteDownload(){
	}
	
	public Object getContextAttribute(String hashAdress,String attribute){
		StartContext context = contextCache.get(hashAdress);
		if (context != null){
			return context.getContextAttribute(attribute);
		}
		return null;
	}

	public void download(String taskName,BasicRequest finalRequest,StartContext finalContext) {
		String hashCode = String.valueOf(finalContext.hashCode());
		if (!contextCache.containsKey(hashCode)){
			contextCache.put(hashCode, finalContext);
		}
		finalRequest.addAttribute("_START_CONTEXT",hashCode);
		IDownload download = loadBalance.getDownload();
		if (download != null){
			try {
				download.dowloadLink(taskName, finalRequest);
			} catch (RemoteException e) {
				logger.warn("invoke remote download", e);
			}
		}
	}
}
