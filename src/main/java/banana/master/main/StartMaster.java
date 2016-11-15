package banana.master.main;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Scanner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import com.alibaba.fastjson.JSON;

import banana.core.modle.MasterConfig;
import banana.core.protocol.CrawlerMasterProtocol;
import banana.core.protocol.Task;
import banana.master.impl.CrawlerMasterServer;

public class StartMaster {

	public static void main(String[] args) throws Exception {
		args = (args == null || args.length == 0) ? new String[] {} : args;
		CommandLineParser parser = new DefaultParser();
		Options options = new Options();
		options.addOption("h", "help", false, "print this usage information");
		options.addOption("c", "config", true, "config file");
		options.addOption("s", "submit", true, "submit task from a jsonfile");
		options.addOption("st", "stoptask", true, "stop a task");
		CommandLine commandLine = parser.parse(options, args);
		HelpFormatter formatter = new HelpFormatter();
		if (commandLine.hasOption('h')) {
			formatter.printHelp("Master", options);
			return;
		}
		if (commandLine.hasOption("st")){
			String taskname = commandLine.getOptionValue("st");
			CrawlerMasterProtocol proxy = (CrawlerMasterProtocol) RPC.getProxy(CrawlerMasterProtocol.class,CrawlerMasterProtocol.versionID,new InetSocketAddress("localhost",8666),new Configuration());
			if (proxy.existTask(taskname).get()){
				proxy.stopTask(taskname);
				System.out.println(taskname + " stoped");
			}else{
				System.out.println(taskname + "not exist");
			}
			return;
		}
		//submit是提交任务
		if (commandLine.hasOption('s')){
			String taskFilePath = commandLine.getOptionValue('s');
			Task task = initOneTask(taskFilePath);
			task.verify();
			boolean resubmit = false;
			Scanner scan = new Scanner(System.in);
			CrawlerMasterProtocol proxy = (CrawlerMasterProtocol) RPC.getProxy(CrawlerMasterProtocol.class,CrawlerMasterProtocol.versionID,new InetSocketAddress("localhost",8666),new Configuration());
			if (proxy.existTask(task.name).get()){
				System.out.print("Name for the task of "+ task.name +" already exists, do you want to update the configuration?\nConfirm the input y/yes:");
				String yes = scan.next();
				if (!yes.equalsIgnoreCase("Y") && !yes.equalsIgnoreCase("YES")){
					System.out.println("Task to submit cancel.");
					return;
				}
				resubmit = true;
			}
			if (proxy.taskdataExists(task.collection, task.name).get()){
				System.out.print("Do you need to remove before fetching result?\nConfirm the input y/yes:");
				String yes = scan.next();
				if (yes.equalsIgnoreCase("Y") || yes.equalsIgnoreCase("YES")){
					String password = null;
					for (int i = 0; i < 3; i++) {
						System.out.print("password:");
						password = scan.next();
						if (password.equals("jisucloud")){
							int n = proxy.removeBeforeResult(task.collection, task.name).get();
							System.out.println("Delete article " + n);
							break;
						}else{
							System.out.println("password error.");
							if (i == 3){
								return;
							}
						}
					}
				}
			}
			if (!resubmit && proxy.statExists(task.collection, task.name).get()){
				System.out.print("Do you need to synchronize the previous links?\nConfirm the input y/yes:");
				String yes = scan.next();
				if (yes.equalsIgnoreCase("Y") || yes.equalsIgnoreCase("YES")){
					task.synchronizeLinks = true;
				}
			}
			proxy.submitTask(task);
			System.out.println("Task to run");
			return;
		}
		//启动master
		File configFile = new File("master_config.json");
		if (commandLine.hasOption("c")){
			configFile = new File(commandLine.getOptionValue("c"));
		}else if (!configFile.exists()){
			try{
				configFile = new File(StartMaster.class.getClassLoader().getResource("").getPath() + "/master_config.json");
			}catch(Exception e){
				System.out.println("请指定配置文件位置");
				System.exit(0);
			}
		}
		MasterConfig config = JSON.parseObject(FileUtils.readFileToString(configFile),MasterConfig.class);
		CrawlerMasterServer crawlerMasterServer = new CrawlerMasterServer(config);
		Server server = new RPC.Builder(new Configuration()).setProtocol(CrawlerMasterProtocol.class)
				.setInstance(crawlerMasterServer).setBindAddress("0.0.0.0").setPort(config.listen).setNumHandlers(config.handlers)
				.build();
		server.start();
		System.out.println("Master已经启动!!!可以陆续启动Downloader来扩展集群了");
	}

	public static Task initOneTask(String path) throws IOException {
		File file = new File(path);
		String json = FileUtils.readFileToString(file, "UTF-8");
		Task task = JSON.parseObject(json, Task.class);
		task.data = json;
		return task;
	}

}
