package dev.nifi.yml;

import java.util.List;

public class RulesYML {
	
	/**
	 * Policy of how FlowFiles are handled when multiple rules are triggered
	 */
	public String policy;

	/**
	 * Advanced rules based on triggers that conditionally generate attributes
	 */
	public List<RuleYML> rules;
	
	public RulesYML(String flowFilePolicy, List<RuleYML> rules) {
		this.policy = flowFilePolicy;
		this.rules = rules;
	}
}
