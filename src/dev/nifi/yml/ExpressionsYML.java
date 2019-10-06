package dev.nifi.yml;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dev.nifi.xml.Actions;

public class ExpressionsYML {

	/**
	 * Map of flowfile attribute names to their values
	 */
	public Map<String, String> expressions;


	public ExpressionsYML(List<Actions> actions) {
		this.expressions = new HashMap<String, String>();
		
		for (Actions action : actions) {
			expressions.put(action.attribute, action.value);
		}
	}
}
