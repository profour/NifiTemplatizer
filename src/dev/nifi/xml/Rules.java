package dev.nifi.xml;

import java.util.List;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;

public class Rules {

	@JacksonXmlElementWrapper(useWrapping = false)
	public List<Conditions> conditions;
	@JacksonXmlElementWrapper(useWrapping = false)
	public List<Actions> actions;

	public String id;
	public String name;
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		
		sb.append("Name: " + name);
		sb.append("\n");
		sb.append("ID: " + id);
		sb.append("\n");
		
		for (Conditions c : conditions) {
			sb.append("- Condition: ");
			sb.append(c);
			sb.append("\n");
		}
		
		for (Actions a : actions) {
			sb.append("- Action: ");
			sb.append(a);
			sb.append("\n");
		}
		
		return sb.toString();
	}
}
