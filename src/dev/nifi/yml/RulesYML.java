package dev.nifi.yml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dev.nifi.xml.Actions;
import dev.nifi.xml.Conditions;

public class RulesYML {

	/**
	 * List of conditions that must all evaluate to true in order for a rule to pass
	 */
	public List<String> conditions;
	
	/**
	 * Map of flowfile attribute names to their values
	 */
	public Map<String, String> actions;


	public RulesYML(List<Conditions> conditions, List<Actions> actions) {
		this.conditions = new ArrayList<String>();
		this.actions = new HashMap<String, String>();
		
		for (Conditions c : conditions) {
			this.conditions.add(c.expression);
		}
		for (Actions action : actions) {
			this.actions.put(action.attribute, action.value);
		}
	}
}
