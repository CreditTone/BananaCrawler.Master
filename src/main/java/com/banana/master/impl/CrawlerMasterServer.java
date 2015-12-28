package com.banana.master.impl;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.banana.common.NodeStatus;
import com.banana.common.download.IDownload;
import com.banana.common.master.ICrawlerMasterServer;
import com.banana.request.BasicRequest;

public final class CrawlerMasterServer extends UnicastRemoteObject implements ICrawlerMasterServer,Runnable {
	
	private static Logger logger = Logger.getLogger(CrawlerMasterServer.class);
	
	private static CrawlerMasterServer master = null;
	
	private long heartCheckInterval = 1000 * 10;
	
	private Map<String,TaskServer> tasks = new HashMap<String, TaskServer>();
	
	private Map<String,IDownload> downloads = new HashMap<String,IDownload>();
	
	private LoadBalance loadBalance = new LoadBalance();
	
	protected CrawlerMasterServer() throws RemoteException {
		super();
	}
	
	static{
		try {
			master = new CrawlerMasterServer();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}
	
	public static CrawlerMasterServer getInstance(){
		return master;
	}
	

	public long getHeartCheckInterval() {
		return heartCheckInterval;
	}

	public void addDownloadNode(String host,int port) throws java.rmi.RemoteException {
		if (downloads.containsKey(host)){
			System.out.println();
			return;
		}
	}

	public void startTask() {
		
	}

	public Object getTaskPropertie(String taskName, String name) throws RemoteException {
		return null;
	}

	public void pushTaskRequests(String taskName, List<BasicRequest> requests) throws RemoteException {
		TaskServer task = tasks.get(taskName);
		if (task != null){
			task.pushRequests(requests);
		}else{
			System.out.println("not found task");
		}
	}

	public LoadBalance getLoadBalance() {
		return loadBalance;
	}
	
	protected IDownload getIDownload(String host){
		IDownload download = downloads.get(host);
		if (download == null)
			throw new NullPointerException();
		return download;
	}


	public void run() {
		while(true){
			List<NodeStatus> lastNodeStatus = new ArrayList<NodeStatus>();
			for (Map.Entry<String,IDownload> entry: downloads.entrySet()) {
				IDownload download = entry.getValue();
				try {
					NodeStatus ns = download.getStatus();
					lastNodeStatus.add(ns);
				} catch (RemoteException e) {
					logger.warn("heartbeat check", e);
					//移除download，通知所有task
				}
			}
			loadBalance.weightCalculating(lastNodeStatus);
			sleep(heartCheckInterval);
		}
	}
	
	private void sleep(long millis){
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
