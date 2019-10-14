package dev.nifi.yml;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import dev.nifi.xml.Actions;
import dev.nifi.xml.Conditions;

public class RuleYML {

	/**
	 * List of conditions that must all evaluate to true in order for a rule to pass
	 */
	public List<String> conditions = new ArrayList<String>();
	
	/**
	 * Map of flowfile attribute names to their values
	 */
	public Map<String, String> actions = new TreeMap<String, String>();

	/**
	 * Don't use. Only for deserialization.
	 */
	public RuleYML() {}

	public RuleYML(List<Conditions> conditions, List<Actions> actions) {
		for (Conditions c : conditions) {
			this.conditions.add(c.expression);
		}
		for (Actions action : actions) {
			this.actions.put(action.attribute, action.value);
		}
	}

}
