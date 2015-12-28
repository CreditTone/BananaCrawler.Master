package com.banana.master.impl;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.banana.common.NodeStatus;
import com.banana.request.BasicRequest;
import com.banana.request.StartContext;

public class LoadBalance {
	
	//private List<RemoteDownload> downloads = new ArrayList<RemoteDownload>();
	
	private List<NodeStatus> lastNodeStatus = new ArrayList<NodeStatus>();
	
	private Map<String,Integer> weights = new HashMap<String,Integer>();
	
	public LoadBalance(){
	}
	
	public void invokeDownload(final BasicRequest finalRequest,final StartContext finalContext) {
		
	}

	
	protected void weightCalculating(List<NodeStatus> lastNodeStatus){
		weights.clear();
		for (NodeStatus ns: lastNodeStatus) {
			double rateMemory = (double)ns.getFreeMemory()/ns.getTotalMemory();
			int weight = (int) (rateMemory * 100);
			weight += ns.getCpuNum() * 2;
			weight -= ns.getActiveThread()/10;
			if (weight < 0){
				weight = 0;
			}
			weights.put(ns.getHost(),weight);
		}
	}
}
