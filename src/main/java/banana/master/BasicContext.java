package banana.master;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.jknack.handlebars.Helper;
import com.github.jknack.handlebars.Options;
import com.github.jknack.handlebars.Template;

import banana.core.ExpandHandlebars;
import banana.core.modle.ContextModle;

public class BasicContext extends HashMap<String,Object> implements ContextModle{
	
	private static final ExpandHandlebars handlebars = new ExpandHandlebars();
	
	static{
		handlebars.registerHelper("existKey", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				String path = options.param(0);
				BasicContext basicContext = (BasicContext) options.context.model();
				return basicContext.existPath(path);
			}
		});
		handlebars.registerHelper("notEmpty", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				String path = options.param(0);
				BasicContext basicContext = (BasicContext) options.context.model();
				return basicContext.existPath(path);
			}
		});
		handlebars.registerHelper("isEmpty", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				String path = options.param(0);
				BasicContext basicContext = (BasicContext) options.context.model();
				return !basicContext.existPath(path);
			}
		});
	}

	@Override
	public Object parseObject(String line) throws IOException {
		if (line.startsWith("{{") && line.endsWith("}}") && !line.contains(" ")){
			return get(line.substring(2, line.length() -2));
		}
		return parseString(line);
	}

	@Override
	public String parseString(String line) throws IOException {
		if (!line.contains("{{")){
			return line;
		}
		Template template = handlebars.compileEscapeInline(line);
		return template.apply(this);
	}

	@Override
	public void copyTo(Map<String, Object> dst) {
		dst.putAll(this);
	}
	
	public boolean existPath(String path) {
		if (path == null){
			return false;
		}
		String[] keys = path.split("\\.");
		Object value = get(keys[0]);
		for (int i = 1; i < keys.length; i++) {
			if (value == null ){
				break;
			}
			if (keys[i].startsWith("[")){
				int index = Integer.parseInt(keys[i].substring(1, keys[i].length()-1));
				value = ((List<Object>)value).get(index);
			}else{
				value = ((Map<String,Object>)value).get(keys[i]);
			}
		}
		if (value != null){
			return !value.equals("");
		}
		return false;
	}

}
