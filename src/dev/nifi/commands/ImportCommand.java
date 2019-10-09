package dev.nifi.commands;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.nifi.api.toolkit.ApiException;
import org.apache.nifi.api.toolkit.api.ProcessGroupsApi;
import org.apache.nifi.api.toolkit.model.BundleDTO;
import org.apache.nifi.api.toolkit.model.FunnelDTO;
import org.apache.nifi.api.toolkit.model.FunnelEntity;
import org.apache.nifi.api.toolkit.model.LabelDTO;
import org.apache.nifi.api.toolkit.model.LabelEntity;
import org.apache.nifi.api.toolkit.model.PortDTO;
import org.apache.nifi.api.toolkit.model.PortEntity;
import org.apache.nifi.api.toolkit.model.PositionDTO;
import org.apache.nifi.api.toolkit.model.ProcessGroupDTO;
import org.apache.nifi.api.toolkit.model.ProcessGroupEntity;
import org.apache.nifi.api.toolkit.model.ProcessorDTO;
import org.apache.nifi.api.toolkit.model.ProcessorEntity;
import org.apache.nifi.api.toolkit.model.RemoteProcessGroupDTO;
import org.apache.nifi.api.toolkit.model.RemoteProcessGroupEntity;
import org.apache.nifi.api.toolkit.model.RevisionDTO;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import dev.nifi.utils.Pair;
import dev.nifi.yml.ElementYML;
import dev.nifi.yml.HelperYML;
import dev.nifi.yml.HelperYML.ReservedComponents;
import dev.nifi.yml.TemplateYML;

public class ImportCommand extends BaseCommand {
	
	private final UUID clientId = UUID.randomUUID();

	private final ProcessGroupsApi processGroupAPI = new ProcessGroupsApi(getApiClient());
	
	private final String importDir;
	
	public ImportCommand(final String importDir) {
		super();
		
		if (importDir == null) {
			this.importDir = ".";
		} else {
			this.importDir = importDir;
		}
	}

	@Override
	public void run() {
		try {
			// Load templates from disk
			List<TemplateYML> templates = loadTemplates(importDir);

			// TODO: Consider using these to determine if all dependencies are available
			// ControllerServiceTypesEntity cs = fapi.getControllerServiceTypes(null, null, null, null, null, null, null);
			// ProcessorTypesEntity pt = fapi.getProcessorTypes(null, null, null);
			
			importTemplates(templates);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ApiException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void importTemplates(List<TemplateYML> templates) throws ApiException {
		
		for (TemplateYML template : templates) {
			if (!template.name.equals("root")) {
				continue;
			}
			Map<String, Pair<String, BundleDTO>> depLookup = createDependencyLookup(template.dependencies);
			
			for (ElementYML ele : template.components) {
				System.out.println(ele.getType());
				Pair<String, BundleDTO> dep = depLookup.get(ele.getType());
				System.out.println(dep);
				PositionDTO position = HelperYML.createPosition(ele.position);
				
				if (dep != null) {
					makeProcessor(ele.id, dep.t1, ele.name, dep.t2, position);
				} else {
					ReservedComponents type = HelperYML.ReservedComponents.valueOf(ele.getType());
					switch (type) {
					case FUNNEL:
					{
						makeFunnel(position, ele);
						break;
					}
					case INPUT_PORT:
					{
						makeInputPort(position, ele);
						break;
					}
					case LABEL:
					{
						makeLabel(position, ele);
						break;
					}
					case OUTPUT_PORT:
					{
						makeOutputPort(position, ele);
						break;
					}
					case PROCESS_GROUP:
					{
						makeProcessGroup(position, ele);
						break;
					}
					case REMOTE_PROCESS_GROUP:
					{
						makeRemoteProcessGroup(position, ele);
						break;
					}
					default:
						// not possible
					}
				}
			}
		}
	}
	
	private void makeFunnel(PositionDTO position, ElementYML ele) throws ApiException {
		FunnelEntity funnel = new FunnelEntity();
		FunnelDTO dto = new FunnelDTO();
		funnel.setComponent(dto);
		funnel.setRevision(getRevision());
		funnel.setId(ele.id);

		dto.setPosition(position);
		
		processGroupAPI.createFunnel("root", funnel);
	}
	
	private void makeProcessGroup(PositionDTO position, ElementYML ele) throws ApiException {
		ProcessGroupEntity pg = new ProcessGroupEntity();
		ProcessGroupDTO dto = new ProcessGroupDTO();
		pg.setComponent(dto);
		pg.setRevision(getRevision());
		pg.setId(ele.id);
		
		dto.setPosition(position);
		dto.setName(ele.name);
		
		processGroupAPI.createProcessGroup("root", pg);
	}
	
	private void makeRemoteProcessGroup(PositionDTO position, ElementYML ele) throws ApiException {
		RemoteProcessGroupEntity rpg = new RemoteProcessGroupEntity();
		RemoteProcessGroupDTO dto = new RemoteProcessGroupDTO();
		rpg.setComponent(dto);
		rpg.setRevision(getRevision());
		rpg.setId(ele.id);
		
		dto.setPosition(position);
		dto.setTargetUris(ele.properties.get("targetUris"));
		
		processGroupAPI.createRemoteProcessGroup("root", rpg);
	}
	
	private void makeOutputPort(PositionDTO position, ElementYML ele) throws ApiException {
		PortEntity port = new PortEntity();
		PortDTO dto = new PortDTO();
		port.setComponent(dto);
		port.setRevision(getRevision());
		port.setId(ele.id);
		
		dto.setPosition(position);
		dto.setName(ele.name);
		
		processGroupAPI.createOutputPort("root", port);
	}
	
	private void makeInputPort(PositionDTO position, ElementYML ele) throws ApiException {
		PortEntity port = new PortEntity();
		PortDTO dto = new PortDTO();
		port.setComponent(dto);
		port.setRevision(getRevision());
		port.setId(ele.id);
		
		dto.setPosition(position);
		dto.setName(ele.name);
		
		processGroupAPI.createInputPort("root", port);
	}
	
	private void makeLabel(PositionDTO position, ElementYML ele) throws ApiException {
		LabelEntity label = new LabelEntity();
		LabelDTO dto = new LabelDTO();
		label.setComponent(dto);
		label.setRevision(getRevision());
		label.setId(ele.id);
		
		dto.setPosition(position);
		dto.setLabel(ele.comment);
		
		processGroupAPI.createLabel("root", label);
	}

	private void makeProcessor(String id, String type, String name, BundleDTO bundle, PositionDTO position) throws ApiException {
		ProcessorEntity p = new ProcessorEntity();
		ProcessorDTO dto = new ProcessorDTO();
		p.setComponent(dto);
		p.setRevision(getRevision());
		p.setId(id);
		
		
		dto.setType(type);
		dto.setBundle(bundle);
		dto.setName(name);
		dto.setPosition(position);
		
		processGroupAPI.createProcessor("root", p);
	}

	
	private static Map<String, Pair<String, BundleDTO>> createDependencyLookup(
			Map<String, Map<String, Map<String, Map<String, String>>>> dependencies) {
		Map<String, Pair<String, BundleDTO>> lookup = new HashMap<>();
		
		for (String group : dependencies.keySet()) {
			Map<String, Map<String, Map<String, String>>> artifacts = dependencies.get(group);
			for (String artifact : artifacts.keySet()) {
				 Map<String, Map<String, String>> versions = artifacts.get(artifact);
				 
				 for (String version : versions.keySet()) {
					 Map<String, String> classes = versions.get(version);
					 
					 BundleDTO bundle = new BundleDTO();
					 bundle.setGroup(group);
					 bundle.setArtifact(artifact);
					 bundle.setVersion(version);
					 
					 for (String canonicalName : classes.keySet()) {
						 String fullType = classes.get(canonicalName);
						 
						 lookup.put(canonicalName, new Pair<String, BundleDTO>(fullType, bundle));
					 }
				 }
			}
		}
		
		return lookup;
	}
	
	private List<TemplateYML> loadTemplates(final String importDir) throws JsonParseException, JsonMappingException, IOException {
		List<TemplateYML> templates = new ArrayList<TemplateYML>();
		
		YAMLFactory f = new YAMLFactory();
		f.enable(YAMLGenerator.Feature.MINIMIZE_QUOTES);
		f.disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER);
		ObjectMapper mapper = new ObjectMapper(f);
		
		File templateDir = new File(importDir);
		
		for (String template : templateDir.list()) {
			if (template.endsWith(".yaml")) {
				TemplateYML yml = mapper.readValue(new File(templateDir.getAbsolutePath() + File.separator + template), TemplateYML.class);
				templates.add(yml);
			}
		}
		
		return templates;
	}
	
	private RevisionDTO getRevision() {
		RevisionDTO rev = new RevisionDTO();
		rev.setClientId(clientId.toString());
		rev.setVersion(0L);
		return rev;
	}
	
	// Tester main method
	public static void main(String[] args) {
		ImportCommand command = new ImportCommand("./examples/simple/");
		command.configureApiClients("localhost", "8080", false);
		command.run();
	}
}
