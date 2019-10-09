package dev.nifi.commands;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.nifi.api.toolkit.model.BundleDTO;
import org.apache.nifi.api.toolkit.model.ControllerServiceDTO;
import org.apache.nifi.api.toolkit.model.ControllerServiceEntity;
import org.apache.nifi.api.toolkit.model.ProcessorDTO;
import org.apache.nifi.api.toolkit.model.ProcessorEntity;

public class DependencyBuilder {


	private Map<String, BundleDTO> bundles = new HashMap<String, BundleDTO>();
	private Map<String, String> types = new HashMap<String, String>();
	
	private Map<String, String> idToProcessorName = new HashMap<String, String>();
	
	
	/**
	 * Lookup the simplified canonical name of this processor's type (nar + version + processor class)
	 * @param componentId 
	 * @return canonical name of the component's dependency
	 */
	public String getCanonicalDependencyName(String componentId) {
		return idToProcessorName.get(componentId);
	}
	
	/**
	 * Convert the information of all dependencies into a nested map 
	 * 	
	 * @return
	 */
	public Map<String, Map<String, Map<String, Map<String, String>>>> build() {
		Map<String, Map<String, Map<String, Map<String, String>>>> groups = new TreeMap<>();

		for (String name : bundles.keySet()) {
			BundleDTO bundle = bundles.get(name);
			String type = types.get(name);
			
			Map<String, Map<String, Map<String, String>>> group = groups.get(bundle.getGroup());
			if (group == null) {
				 group = new TreeMap<>();
				 groups.put(bundle.getGroup(), group);
			}

			Map<String, Map<String, String>> artifact = group.get(bundle.getArtifact());
			if (artifact == null) {
				artifact = new TreeMap<>();
				group.put(bundle.getArtifact(), artifact);
			}
			
			Map<String, String> version = artifact.get(bundle.getVersion());
			if (version == null) {
				version = new TreeMap<>();
				artifact.put(bundle.getVersion(), version);
			}
			
			version.put(name, type);
		}
		
		return groups;
	}

	public DependencyBuilder addDependency(ProcessorEntity processor) {
		ProcessorDTO dto = processor.getComponent();
		
		// Convert the dependency into a simple name that can be referenced by all components
		String canonicalName = processDependency(dto.getType(), dto.getBundle());
		
		// Store the correspondence between component id -> canonical name of its dependency
		idToProcessorName.put(processor.getId(), canonicalName);
		
		return this;
	}
	
	public DependencyBuilder addDependency(ControllerServiceEntity controller) {
		ControllerServiceDTO dto = controller.getComponent();

		// Convert the dependency into a simple name that can be referenced by all components
		String canonicalName = processDependency(dto.getType(), dto.getBundle());
		
		// Store the correspondence between component id -> canonical name of its dependency
		idToProcessorName.put(controller.getId(), canonicalName);
		
		return this;
	}
	
	public DependencyBuilder addAllProcessorDependencies(List<ProcessorEntity> processors) {
		for (ProcessorEntity processor : processors) {
			addDependency(processor);
		}
		return this;
	}
	
	public DependencyBuilder addAllControllerDependencies(List<ControllerServiceEntity> controllers) {
		for (ControllerServiceEntity controller : controllers) {
			addDependency(controller);
		}
		return this;
	}
	
	
	private String processDependency(String type, BundleDTO bundle) {

		String[] classparts = type.split("\\.");
		String className = classparts[classparts.length-1];
		String canonicalName = className;
		
		int i = 1;
		
		while (bundles.containsKey(canonicalName)) {
			BundleDTO namedBundle = bundles.get(canonicalName);
			String namedType = types.get(canonicalName);
			
			// Check to see if the named bundle is the exact same as what we are trying to add
			// If so, nothing needs to be done and the name is fine
			if (namedType.equals(type) &&
				namedBundle.getArtifact().equals(bundle.getArtifact()) &&
				namedBundle.getGroup().equals(bundle.getGroup()) &&
				namedBundle.getVersion().equals(bundle.getVersion())) {
				break;
			}
			
			// If we had a collision with an existing named bundle, increment to the next canonical name and check if that collides
			canonicalName = className + "#" + i++;
		}
		
		// Store the canonical name that didn't have any collisions
		bundles.put(canonicalName, bundle);
		types.put(canonicalName, type);
		
		return canonicalName;
	}
	
}
