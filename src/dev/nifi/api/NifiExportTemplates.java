package dev.nifi.api;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.nifi.api.toolkit.ApiException;
import org.apache.nifi.api.toolkit.api.FlowApi;
import org.apache.nifi.api.toolkit.api.ProcessGroupsApi;
import org.apache.nifi.api.toolkit.model.BundleDTO;
import org.apache.nifi.api.toolkit.model.ConnectionEntity;
import org.apache.nifi.api.toolkit.model.ConnectionsEntity;
import org.apache.nifi.api.toolkit.model.ControllerServiceDTO;
import org.apache.nifi.api.toolkit.model.ControllerServiceEntity;
import org.apache.nifi.api.toolkit.model.ControllerServicesEntity;
import org.apache.nifi.api.toolkit.model.FunnelEntity;
import org.apache.nifi.api.toolkit.model.FunnelsEntity;
import org.apache.nifi.api.toolkit.model.InputPortsEntity;
import org.apache.nifi.api.toolkit.model.OutputPortsEntity;
import org.apache.nifi.api.toolkit.model.PortEntity;
import org.apache.nifi.api.toolkit.model.ProcessGroupEntity;
import org.apache.nifi.api.toolkit.model.ProcessGroupsEntity;
import org.apache.nifi.api.toolkit.model.ProcessorDTO;
import org.apache.nifi.api.toolkit.model.ProcessorEntity;
import org.apache.nifi.api.toolkit.model.ProcessorsEntity;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import dev.nifi.yml.ControllerYML;
import dev.nifi.yml.ProcessorYML;
import dev.nifi.yml.TemplateYML;


public class NifiExportTemplates {


	public static void main(String[] args) throws ApiException, JsonGenerationException, JsonMappingException, IOException {
		
		// Start from the root and work our way down converting all process groups into templates
		List<TemplateYML> templates = new ArrayList<TemplateYML>();
		convertToTemplateYML("root", templates);
		
		YAMLFactory f = new YAMLFactory();
		f.enable(YAMLGenerator.Feature.MINIMIZE_QUOTES);
		f.disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER);
		ObjectMapper mapper = new ObjectMapper(f);
		
		mapper.setSerializationInclusion(Include.NON_EMPTY);
		
		for (TemplateYML template : templates) {
			String yaml = mapper.writer().writeValueAsString(template);
			
			// Formatting to make it easier to read the templates
			yaml = yaml.replaceAll("\n-", "\n\n-");
			yaml = yaml.replace("\ndependencies:", "\n\ndependencies:");
			yaml = yaml.replace("\ncontrollers:", "\n\ncontrollers:");
			yaml = yaml.replace("\ncontrollers:\n", "\ncontrollers:");
			yaml = yaml.replace("\ncomponents:", "\n\ncomponents:");
			yaml = yaml.replace("\ncomponents:\n", "\ncomponents:");
			
			try (FileWriter writer = new FileWriter(template.name + ".yaml")) {
				writer.write(yaml);
			}
		}
	}
	
	public static TemplateYML convertToTemplateYML(String processGroupId, List<TemplateYML> allTemplates) throws ApiException {
		FlowApi fapi = new FlowApi();
		fapi.getApiClient().setBasePath("http://localhost:8080/nifi-api");
		
		ProcessGroupsApi pgapi = new ProcessGroupsApi();
		pgapi.getApiClient().setBasePath("http://localhost:8080/nifi-api");
		
		// Pull all of the information we need to construct the template
		ProcessorsEntity root = pgapi.getProcessors(processGroupId, false);
		ConnectionsEntity connections = pgapi.getConnections(processGroupId);
		FunnelsEntity funnels = pgapi.getFunnels(processGroupId);
		ProcessGroupsEntity pge = pgapi.getProcessGroups(processGroupId);
		InputPortsEntity ipe = pgapi.getInputPorts(processGroupId);
		OutputPortsEntity ope = pgapi.getOutputPorts(processGroupId);
		ControllerServicesEntity cse = fapi.getControllerServicesFromGroup(processGroupId, false, false);
		
		
		Map<String, List<ConnectionEntity>> connectionLookup = new HashMap<>();
		for (ConnectionEntity ce : connections.getConnections()) {
			String destination = ce.getDestinationId();
			if (!connectionLookup.containsKey(destination)) {
				connectionLookup.put(destination, new ArrayList<>());
			}
			
			connectionLookup.get(destination).add(ce);
		}
		
		
		// Start to convert into YAML
		TemplateYML rootPG = new TemplateYML();
		rootPG.name = processGroupId;
		allTemplates.add(rootPG);
		
		// Generate all of the dependencies needed
		Map<String, String> idToProcessorName = generateDependencies(rootPG, root.getProcessors(), cse.getControllerServices());
		
		for (ControllerServiceEntity controller : cse.getControllerServices()) {
			ControllerYML c = new ControllerYML(controller);
			rootPG.controllers.add(c);
		}
		
		for (PortEntity port : ipe.getInputPorts()) {
			ProcessorYML p = new ProcessorYML(port, connectionLookup.get(port.getId()));
			rootPG.components.add(p);
		}
		for (PortEntity port : ope.getOutputPorts()) {
			ProcessorYML p = new ProcessorYML(port, connectionLookup.get(port.getId()));
			rootPG.components.add(p);
		}
		
		for (ProcessorEntity pe : root.getProcessors()) {
			ProcessorYML p = new ProcessorYML(pe, idToProcessorName.get(pe.getId()), connectionLookup.get(pe.getId()));
			rootPG.components.add(p);
		}
		
		for (FunnelEntity f : funnels.getFunnels()) {
			ProcessorYML p = new ProcessorYML(f, connectionLookup.get(f.getId()));
			rootPG.components.add(p);
		}
		
		for (ProcessGroupEntity pg : pge.getProcessGroups()) {
			TemplateYML template = convertToTemplateYML(pg.getId(), allTemplates);
			
			ProcessorYML p = new ProcessorYML(pg, template.name, connectionLookup.get(pg.getId()));
			rootPG.components.add(p);
		}
		
		return rootPG;
	}
	
	public static Map<String, String> generateDependencies(TemplateYML yml, List<ProcessorEntity> processors, List<ControllerServiceEntity> controllers) {
		Map<String, Map<String, Map<String, Map<String, String>>>> groups = yml.dependencies;

		
		Map<String, BundleDTO> bundles = new HashMap<String, BundleDTO>();
		Map<String, String> types = new HashMap<String, String>();
		
		Map<String, String> idToProcessorName = new HashMap<String, String>();
		for (ProcessorEntity pe : processors) {
			ProcessorDTO dto = pe.getComponent();
			
			String[] classparts = dto.getType().split("\\.");
			String className = classparts[classparts.length-1];
			String name = className;
			int i = 2;
			while (bundles.containsKey(dto.getName())) {
				
				//Don't need to do anything if they are the exact same
				BundleDTO b = bundles.get(name);
				String type = types.get(name);
				if (type.equals(dto.getType()) &&
					b.getArtifact().equals(dto.getBundle().getArtifact()) &&
					b.getGroup().equals(dto.getBundle().getGroup()) &&
					b.getVersion().equals(dto.getBundle().getVersion())) {
					break;
				}
				
				name = className + "#" + i;
				++i;
			}
			
			bundles.put(name, dto.getBundle());
			types.put(name, dto.getType());
			idToProcessorName.put(pe.getId(), name);
		}
		for (ControllerServiceEntity cse : controllers) {
			ControllerServiceDTO dto = cse.getComponent();
			
			String[] classparts = dto.getType().split("\\.");
			String className = classparts[classparts.length-1];
			String name = className;
			int i = 2;
			while (bundles.containsKey(dto.getName())) {
				
				//Don't need to do anything if they are the exact same
				BundleDTO b = bundles.get(name);
				String type = types.get(name);
				if (type.equals(dto.getType()) &&
					b.getArtifact().equals(dto.getBundle().getArtifact()) &&
					b.getGroup().equals(dto.getBundle().getGroup()) &&
					b.getVersion().equals(dto.getBundle().getVersion())) {
					break;
				}
				
				name = className + "#" + i;
				++i;
			}
			
			bundles.put(name, dto.getBundle());
			types.put(name, dto.getType());
			idToProcessorName.put(cse.getId(), name);
		}
		
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
		
		return idToProcessorName;
	}
	
}
