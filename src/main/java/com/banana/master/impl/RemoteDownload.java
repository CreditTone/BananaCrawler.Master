package com.banana.master.impl;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.banana.common.NodeStatus;
import com.banana.common.download.IDownload;
import com.banana.request.BasicRequest;
import com.banana.request.StartContext;

public class RemoteDownload {
	
	private final String taskName;
	
	private final IDownload download;
	
	private Map<String,StartContext> contextCache = new ConcurrentHashMap<String,StartContext>();
	
	public RemoteDownload(String taskName,String host) {
		this.taskName = taskName;
		this.download = CrawlerMasterServer.getInstance().getIDownload(host);
	}
	
	public IDownload getDownload() {
		return download;
	}
	public Object getContextAttribute(String hashAdress,String attribute){
		StartContext context = contextCache.get(hashAdress);
		if (context != null){
			return context.getContextAttribute(attribute);
		}
		return null;
	}

	private void download(String taskName,BasicRequest finalRequest,StartContext finalContext) throws RemoteException {
		String hashCode = String.valueOf(finalContext.hashCode());
		if (!contextCache.containsKey(hashCode)){
			contextCache.put(hashCode, finalContext);
		}
		finalRequest.addAttribute("_START_CONTEXT",hashCode);
		download.dowloadLink(taskName, finalRequest);
	}
}
