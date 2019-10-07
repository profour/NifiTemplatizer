package dev.nifi.yml;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TemplateYML {

	public String name;
	
	public Map<String, Map<String, Map<String, Map<String, String>>>> dependencies = new TreeMap<>();
	
	public List<ControllerYML> controllers = new ArrayList<ControllerYML>();
	
	public List<ElementYML> components = new ArrayList<ElementYML>();
		
	
	public TemplateYML() {}
}
