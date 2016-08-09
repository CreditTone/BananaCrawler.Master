package com.banana.master.main;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.banana.master.impl.CrawlerMasterServer;

import banana.standalone.common.protocol.CrawlerMasterProtocol;


public class StartMaster {

	public static void main(String[] args) throws RemoteException, MalformedURLException, ParseException {
		args = (args == null || args.length == 0)?new String[]{"-h"}:args;
		CommandLineParser parser = new BasicParser( );  
		Options options = new Options();  
		options.addOption("h", "help", false, "Print this usage information");  
		options.addOption("r", "redis", true, "Set the redis service. For example: 127.0.0.1:6379");
		CommandLine commandLine = parser.parse( options, args ); 
		HelpFormatter formatter = new HelpFormatter();
		if(commandLine.hasOption('h') ) {
		    formatter.printHelp("Master", options);
		    System.exit(0);
		}
		String redis = "localhost";
		int redisPort = 6379;
		if(commandLine.hasOption('r') ) {
			redis = commandLine.getOptionValue("r").split(":")[0];
			redisPort = Integer.parseInt(commandLine.getOptionValue("r").split(":")[1]);
		}
		CrawlerMasterServer.init(redis, redisPort);
		CrawlerMasterProtocol crawlerMasterServer = CrawlerMasterServer.getInstance();
		if (crawlerMasterServer != null){
			LocateRegistry.createRegistry(1099);
			Naming.rebind("rmi://localhost:1099/master", crawlerMasterServer);
			System.out.println("Master已经启动!!!你可以陆续启动Downloader来扩展集群了");
		}
	}
	
}
