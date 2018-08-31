package banana.master.task;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import banana.core.context.ExpandHandlebars;
import banana.core.request.HttpRequest;
import banana.core.request.HttpRequest.Method;
import banana.core.request.RequestBuilder;
import banana.master.MasterServer;

public class SeedQuery {
	
	public banana.core.modle.Task.SeedQuery seed_query;
	
	private String collection;
	
	private DBObject ref;
	
	private DBObject keys;
	
	private int limit = 10000;
	
	private String sql;
	
	private boolean canQuery = true;
	
	public SeedQuery(String collection,banana.core.modle.Task.SeedQuery seed_query){
		this.collection = collection;
		this.seed_query = seed_query;
		if (seed_query.find.containsKey("sql")){
			sql = (String) seed_query.find.get("sql");
		}else{
			Map<String,Object> querys = (Map<String, Object>) seed_query.find.get("ref");
			ref = new BasicDBObject(querys);
			if (seed_query.find.containsKey("keys")){
				keys = new BasicDBObject((Map<String, Object>)seed_query.find.get("keys"));
			}
			if (seed_query.find.containsKey("limit")){
				limit = (int) seed_query.find.get("limit");
			}
		}
	}
	
	public List<HttpRequest> query() throws Exception{
		if (!canQuery){
			return new ArrayList<HttpRequest>();
		}
		if (!seed_query.keep){
			canQuery = false;
		}
		if (sql != null){
			return sqlQuery();
		}else{
			return mongoDBFind();
		}
	}
	
	private List<HttpRequest> sqlQuery(){
		List<HttpRequest> reqs = new ArrayList<HttpRequest>();
		Statement statement = null;
		ResultSet result = null;
		try{
			statement = MasterServer.getInstance().getSqlConnection().createStatement();
			result = statement.executeQuery(sql);
			ResultSetMetaData metaData = result.getMetaData();
			ExpandHandlebars handlebar = new ExpandHandlebars();
			while(result.next()){
				Map<String,Object> context = new HashMap<String,Object>();
				for (int i = 1; i < metaData.getColumnCount(); i++) {
					context.put(metaData.getColumnName(i), result.getObject(i));
				}
				HttpRequest request = createRequest(handlebar, context);
				reqs.add(request);
			}
			if (reqs.size() == 0){
				canQuery = false;
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if (result != null){
				try {
					result.close();
				} catch (SQLException e) {
				}
			}
			if (statement != null){
				try {
					statement.close();
				} catch (SQLException e) {
				}
			}
		}
		return reqs;
	}
	
	private List<HttpRequest> mongoDBFind() throws Exception{
		Thread.sleep(3 * 1000);
		List<HttpRequest> result = new ArrayList<HttpRequest>();
		DBCursor cursor = MasterServer.getInstance().getMongoDB().getCollection(collection).find(ref, keys).limit(limit);
		if (cursor.count() == 0){
			canQuery = false;
			return result;
		}
		ExpandHandlebars handlebar = new ExpandHandlebars();
		while(cursor.hasNext()){
			Map<String,Object> context = cursor.next().toMap();
			HttpRequest request = createRequest(handlebar, context);
			result.add(request);
		}
		return result;
	}

	private HttpRequest createRequest(ExpandHandlebars handlebar, Map<String, Object> context) throws IOException {
		HttpRequest request = null;
		String url = null;
		if (seed_query.url != null){
			url = handlebar.escapeParse(seed_query.url, context);
			request = RequestBuilder.custom().setUrl(url).setProcessor(seed_query.processor).build();
		}else if(seed_query.download != null){
			url = handlebar.escapeParse(seed_query.download, context);
			request = RequestBuilder.custom().setDownload(url).setProcessor(seed_query.processor).build();
		}
		context.remove("_id");
		for(Entry<String,Object> entry : context.entrySet()){
			request.addAttribute(entry.getKey(), entry.getValue());
		}
		if (seed_query.method != null && seed_query.method.equalsIgnoreCase("POST")){
			request.setMethod(Method.POST);
		}
		if (seed_query.headers != null){
			for (Entry<String,String> entry : seed_query.headers.entrySet()) {
				String value = handlebar.escapeParse(entry.getValue(), context);
				request.putHeader(entry.getKey(), value);
			}
		}
		if (seed_query.params != null){
			for (Entry<String,String> entry : seed_query.params.entrySet()) {
				String value = handlebar.escapeParse(entry.getValue(), context);
				request.putParams(entry.getKey(), value);
			}
		}
		return request;
	}
	
	public final boolean canQuery(){
		return canQuery;
	}
	
}
