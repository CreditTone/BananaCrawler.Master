package com.banana.master.main;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

import com.banana.common.master.ICrawlerMasterServer;
import com.banana.master.impl.CrawlerMasterServer;


public class StartMaster {

	public static void main(String[] args) throws RemoteException, MalformedURLException {
		CrawlerMasterServer.init("127.0.0.1", 6379);
		ICrawlerMasterServer crawlerMasterServer = CrawlerMasterServer.getInstance();
		LocateRegistry.createRegistry(1099);
		Naming.rebind("rmi://localhost:1099/master", crawlerMasterServer);
	}

}
