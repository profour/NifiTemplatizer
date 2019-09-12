package dev.nifi.api;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.api.toolkit.model.BundleDTO;

public class Dependency {

	private static final Map<String, Dependency> cachedDeps = new HashMap<String, Dependency>();
	
	
	private final String group;
	private final String artifact;
	private final String version;
	private final String processor;
	
	private String alias;
	
	public Dependency(String group, String artifact, String version, String processor) {
		this.group = group;
		this.artifact = artifact;
		this.version = version;
		this.processor = processor;
		
		this.alias = null;
	}
	
	public Dependency(String group, String artifact, String version, String processor, String alias) {
		this(group, artifact, alias, processor);
		
		this.alias = alias;
	}
	
	public String getAlias() {
		if (alias != null && !alias.isEmpty()) {
			return alias;
		} else {
			return getUniqueName(group, artifact, version, processor);
		}
	}
	
	private void write(OutputStreamWriter writer) throws IOException {
		writer.append("Dependency:\n");
		writer.append("\t- Group: " + group + "\n");
		writer.append("\t- Artifact: " + artifact + "\n");
		writer.append("\t- Version: " + version + "\n");
		if (alias != null && !alias.isEmpty()) {
			writer.append("\t- Alias: " + alias);
		}
	}

	public static Dependency find(BundleDTO bundle, String processorType) {
		
		String key = getUniqueName(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion(), processorType);
		if (!cachedDeps.containsKey(key)) {
			cachedDeps.put(key, new Dependency(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion(), processorType));
		}
		
		return cachedDeps.get(key);
	}
	
	public static Dependency findAlias(String alias) {
		return cachedDeps.get(alias);
	}
	
	private static String getUniqueName(String group, String artifact, String version, String processorType) {
		 return String.join("/", group, artifact, version, processorType);
	}
	
	public static void writeDependencies(OutputStreamWriter writer) throws IOException {
		
		List<String> depNames = new ArrayList<String>(cachedDeps.keySet());
		Collections.sort(depNames);
		
		for (String dep : depNames) {
			Dependency d = cachedDeps.get(dep);
			
			d.write(writer);
		}
		
	}
	
	public static void readDependencies(File f) {

	}
	
}
