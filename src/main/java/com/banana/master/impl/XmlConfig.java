package com.banana.master.impl;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import com.banana.component.config.XmlConfigPageProcessor;
import com.banana.queue.DelayedBlockingQueue;
import com.banana.queue.DelayedPriorityBlockingQueue;
import com.banana.queue.RedisRequestBlockingQueue;
import com.banana.queue.RequestPriorityBlockingQueue;
import com.banana.queue.SimpleBlockingQueue;
import com.banana.request.PageRequest;
import com.banana.request.StartContext;

public class XmlConfig {
	
	private String name;

	private Integer thread;
	
	private List<String> downloadHosts;
	
	private String queueClassName;
	
	private int delayInMilliseconds = -1;
	
	private StartContext startContext;
	
	
	public static XmlConfig loadXmlConfig(String xml) throws Exception{
		XmlConfig config = new XmlConfig();
		SAXBuilder builder = new SAXBuilder();
		Document document = builder.build(new StringReader(xml));
		Element root = document.getRootElement();
		config.name = root.getAttributeValue("name");
		Element queue = root.getChild("Queue");
		if (queue != null && queue.hasAttributes()){
			String queueType = queue.getAttributeValue("type");
			switch(queueType){
			case "DelayedPriorityBlockingQueue":
				config.delayInMilliseconds = Integer.parseInt(queue.getChildText("DelayInMilliseconds"));
				config.queueClassName = DelayedPriorityBlockingQueue.class.getName();
				break;
			case "DelayedBlockingQueue":
				config.delayInMilliseconds = Integer.parseInt(queue.getTextTrim());
				config.queueClassName = DelayedBlockingQueue.class.getName();
				break;
			case "RequestPriorityBlockingQueue":
				config.queueClassName = RequestPriorityBlockingQueue.class.getName();
				break;
			case "RedisRequestBlockingQueue":
				config.queueClassName = RedisRequestBlockingQueue.class.getName();
				break;
			default:
				config.queueClassName = SimpleBlockingQueue.class.getName();
				break;
			}
		}
		Element seed = root.getChild("StartContext");
		List<Element> requests = seed.getChildren("PageRequest");
		if (requests.isEmpty()){
			throw new Exception("Please configure the seeds");
		}
		config.startContext = new StartContext();
		for (Element request : requests) {
			PageRequest req = config.startContext.createPageRequest(request.getChildText("Url"), XmlConfigPageProcessor.class);
			req.setProcessorAddress(request.getAttributeValue("processor"));
			config.startContext.injectSeed(req);
		}
		Element downloadControl = root.getChild("DownloadControl");
		if (downloadControl != null){
			List<Element> downloadEles = downloadControl.getChildren("Download");
			if (!downloadEles.isEmpty()){
				config.downloadHosts = new ArrayList<String>();
				for (Element elm : downloadEles) {
					config.downloadHosts.add(elm.getTextTrim());
				}
			}
		}
		Element thread = root.getChild("Thread");
		if (thread != null){
			config.thread = Integer.parseInt(thread.getTextTrim());
			//校验线程数不能小于download实例数量
			int downloadCount = config.downloadHosts != null?config.downloadHosts.size():CrawlerMasterServer.getInstance().getDownloads().size();
			if (config.thread < downloadCount){
				throw new Exception("The number of threads cannot be less than the downloader");
			}
		}
		return config;
	}


	public String getName() {
		return name;
	}


	public Integer getThread() {
		return thread;
	}


	public List<String> getDownloadHosts() {
		return downloadHosts;
	}


	public String getQueueClassName() {
		return queueClassName;
	}


	public int getDelayInMilliseconds() {
		return delayInMilliseconds;
	}


	public StartContext getStartContext() {
		return startContext;
	}

}
