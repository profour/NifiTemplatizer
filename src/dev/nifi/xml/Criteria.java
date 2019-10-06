package dev.nifi.xml;

import java.util.List;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

/**
 * Root XML element of the NiFi annotation found on processors with advanced rules configured.
 */
public class Criteria {
	public String flowFilePolicy;
	
	@JacksonXmlProperty(localName = "rules")
	@JacksonXmlElementWrapper(useWrapping = false)
	public List<Rules> rules;
	
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("Policy: ");
		sb.append(flowFilePolicy);
		sb.append("\n");

		sb.append("=================\n");
		for (Rules r : rules) {
			sb.append(r);
			sb.append("\n");
		}
		
		return sb.toString();
	}
}
