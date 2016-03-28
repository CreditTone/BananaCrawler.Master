package com.banana.master.impl;

import java.io.Closeable;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import com.banana.common.NodeStatus;
import com.banana.common.download.IDownload;

public class HealthDetector extends TimerTask implements Closeable{
	
	private Timer timer = new Timer();  
	
	private long heartCheckCycle = 1000 * 10;
	
	private Map<String,IDownload> downloads = new HashMap<String,IDownload>();
	
	private Map<String,LinkedList<NodeStatus>> lastNodeStatus = new HashMap<String,LinkedList<NodeStatus>>();
	
	//private Map<IDownload,Integer> weights = new HashMap<IDownload,Integer>();
	
	public void listenDownloader(String host,IDownload download){
		if (!downloads.containsKey(host)){
			downloads.put(host, download);
			lastNodeStatus.put(host, new LinkedList<NodeStatus>());
		}
	}
	
	public void removeDownloader(String host){
		downloads.remove(host);
		lastNodeStatus.remove(host);
	}
	
	public void start(){
		timer.schedule(this, heartCheckCycle);
	}

	@Override
	public void close() throws IOException {
		timer.cancel();
	}

	@Override
	public void run() {
		Set<Entry<String,IDownload>> entrys = downloads.entrySet();
		for (Entry<String,IDownload> entry : entrys) {
			NodeStatus ns;
			try {
				ns = entry.getValue().healthCheck();
				addNodeStatus(entry.getKey(), ns);
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		}
	}
	
	private synchronized void addNodeStatus(String host,NodeStatus nodeStatus){
		LinkedList<NodeStatus> status = lastNodeStatus.get(host);
		if (status.size() == 10){
			status.removeLast();
		}
		status.addFirst(nodeStatus);
	}

	
//	protected void weightCalculating(){
//		weights.clear();
//		for (Map.Entry<IDownload, NodeStatus> entry: lastNodeStatus.entrySet()) {
//			NodeStatus ns = entry.getValue();
//			double rateMemory = (double)ns.getFreeMemory()/ns.getTotalMemory();
//			int weight = (int) (rateMemory * 100);
//			weight += ns.getCpuNum() * 2;
//			weight -= ns.getActiveThread()/10;
//			if (weight < 0){
//				weight = 0;
//			}
//			weights.put(entry.getKey(),weight);
//		}
//	}
}
