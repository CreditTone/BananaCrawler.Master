package banana.master.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import banana.core.ExpandHandlebars;
import banana.core.request.HttpRequest;
import banana.core.request.PageRequest;
import banana.core.request.RequestBuilder;
import banana.master.impl.CrawlerMasterServer;

public class SeedQuerys {
	
	public banana.core.protocol.Task.SeedQuery seed_query;
	
	private String collection;
	
	private DBObject ref;
	
	private DBObject keys;
	
	private int limit = 10000;
	
	private boolean canQuery = true;
	
	public SeedQuerys(String collection,banana.core.protocol.Task.SeedQuery seed_query){
		this.collection = collection;
		this.seed_query = seed_query;
		Map<String,Object> querys = (Map<String, Object>) seed_query.find.get("ref");
		ref = new BasicDBObject(querys);
		if (seed_query.find.containsKey("keys")){
			keys = new BasicDBObject((Map<String, Object>)seed_query.find.get("keys"));
		}
		if (seed_query.find.containsKey("limit")){
			limit = (int) seed_query.find.get("limit");
		}
		
	}
	
	public List<HttpRequest> query() throws Exception{
		Thread.sleep(10 * 1000);
		List<HttpRequest> result = new ArrayList<HttpRequest>();
		if (!canQuery){
			return result;
		}
		DBCursor cursor = CrawlerMasterServer.getInstance().db.getCollection(collection).find(ref, keys).limit(limit);
		if (cursor.count() == 0){
			canQuery = false;
			return result;
		}
		ExpandHandlebars handlebar = new ExpandHandlebars();
		while(cursor.hasNext()){
			Map<String,Object> dbObject = cursor.next().toMap();
			HttpRequest request = null;
			String url = null;
			if (seed_query.url != null){
				url = handlebar.escapeParse(seed_query.url, dbObject);
				request = RequestBuilder.createPageRequest(url, seed_query.processor);
			}else if(seed_query.download != null){
				url = handlebar.escapeParse(seed_query.download, dbObject);
				request = RequestBuilder.createBinaryRequest(url, seed_query.processor);
			}
			dbObject.remove("_id");
			for(Entry<String,Object> entry : dbObject.entrySet()){
				request.addAttribute(entry.getKey(), entry.getValue());
			}
			request.setMethod(request.getMethod());
			if (seed_query.headers != null){
				for (Entry<String,String> entry : seed_query.headers.entrySet()) {
					String value = handlebar.escapeParse(entry.getValue(), dbObject);
					request.putHeader(entry.getKey(), value);
				}
			}
			if (seed_query.params != null){
				for (Entry<String,String> entry : seed_query.params.entrySet()) {
					String value = handlebar.escapeParse(entry.getValue(), dbObject);
					request.putParams(entry.getKey(), value);
				}
			}
			result.add(request);
		}
		if (!seed_query.keep){
			canQuery = false;
		}
		return result;
	}
	
	public final boolean canQuery(){
		return canQuery;
	}
	
}
