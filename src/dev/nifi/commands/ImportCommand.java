package dev.nifi.commands;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.nifi.api.toolkit.ApiException;
import org.apache.nifi.api.toolkit.api.ConnectionsApi;
import org.apache.nifi.api.toolkit.api.ProcessGroupsApi;
import org.apache.nifi.api.toolkit.model.BundleDTO;
import org.apache.nifi.api.toolkit.model.ConnectableDTO;
import org.apache.nifi.api.toolkit.model.ConnectionDTO;
import org.apache.nifi.api.toolkit.model.ConnectionEntity;
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
import dev.nifi.yml.InputConnectionYML;
import dev.nifi.yml.HelperYML.ReservedComponents;
import dev.nifi.yml.TemplateYML;

public class ImportCommand extends BaseCommand {

	private final ProcessGroupsApi processGroupAPI = new ProcessGroupsApi(getApiClient());
	private final ConnectionsApi connectionAPI = new ConnectionsApi(getApiClient());
	
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
			
			// Build a lookup database for template names
			Map<String, TemplateYML> templateDB = new HashMap<>();
			for (TemplateYML template : templates) {
				// References are by filename so add the extension ".yaml" for simple lookups
				templateDB.put(template.name + HelperYML.YAML_EXT, template); 
			}
			
			// Start with the root template
			importTemplate("root", templateDB.get("root"), templateDB);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ApiException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getResponseBody());
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void importTemplate(String processGroupId, TemplateYML template, Map<String, TemplateYML> templateDB) throws ApiException {
		
		createElements(processGroupId, template, templateDB);
		
		//createLinkage(processGroupId, template);
		
	}
	
	private void createElements(String processGroupId, TemplateYML template, Map<String, TemplateYML> templateDB) throws ApiException {
		// Canonical name -> (Type, Bundle)
		Map<String, Pair<String, BundleDTO>> depLookup = createDependencyLookup(template.dependencies);
		
		for (ElementYML ele : template.components) {
			System.out.println(ele.getType());
			Pair<String, BundleDTO> dep = depLookup.get(ele.getType());
			System.out.println(dep);
			PositionDTO position = HelperYML.createPosition(ele.position);
			
			if (dep != null) {
				makeProcessor(processGroupId, ele, dep, position);
			} else {
				ReservedComponents type = HelperYML.ReservedComponents.valueOf(ele.getType());
				switch (type) {
				case FUNNEL:
				{
					makeFunnel(processGroupId, position, ele);
					break;
				}
				case INPUT_PORT:
				{
					makeInputPort(processGroupId, position, ele);
					break;
				}
				case LABEL:
				{
					makeLabel(processGroupId, position, ele);
					break;
				}
				case OUTPUT_PORT:
				{
					makeOutputPort(processGroupId, position, ele);
					break;
				}
				case PROCESS_GROUP:
				{
					makeProcessGroup(processGroupId, position, ele, templateDB);
					break;
				}
				case REMOTE_PROCESS_GROUP:
				{
					makeRemoteProcessGroup(processGroupId, position, ele);
					break;
				}
				default:
					// not possible
				}
			}
		}
	}
	
	private void createLinkage(String processGroupId, TemplateYML template) throws ApiException {
		
		Map<String, ElementYML> lookup = new HashMap<String, ElementYML>();
		for (ElementYML element : template.components) {
			lookup.put(element.id, element);
		}
		
		// For all components, connect their inputs
		for (ElementYML destinationElement : template.components) {
			if (destinationElement.inputs == null) {
				continue;
			}
			
			for (InputConnectionYML input : destinationElement.inputs) {
				ElementYML sourceElement = lookup.get(input.source);
				
				String connectionId = UUID.randomUUID().toString();
				
				ConnectionEntity conn = new ConnectionEntity();
				conn.setRevision(getRevision());
				
				ConnectionDTO dto = new ConnectionDTO();
				
				// Set the source of the connection
				ConnectableDTO src = new ConnectableDTO();
				if (HelperYML.ReservedComponents.PROCESS_GROUP.isType(sourceElement.type) || 
						HelperYML.ReservedComponents.REMOTE_PROCESS_GROUP.isType(sourceElement.type)) {
					src.setGroupId(sourceElement.id);
					src.setId(sourceElement.id);
				} else {
					src.setGroupId(processGroupId);
					src.setId(sourceElement.id);
				}
				dto.setSource(src);
				
				// Set the destination of the connection
				ConnectableDTO dst = new ConnectableDTO();
				dto.setDestination(dst);
				
				conn.setComponent(dto);
				
				connectionAPI.updateConnection(connectionId, conn);
			}
		}
	}
	
	private void makeFunnel(String processGroupId, PositionDTO position, ElementYML ele) throws ApiException {
		FunnelEntity funnel = new FunnelEntity();
		FunnelDTO dto = new FunnelDTO();
		funnel.setComponent(dto);
		funnel.setRevision(getRevision());
		funnel.setId(ele.id);

		dto.setPosition(position);
		
		processGroupAPI.createFunnel(processGroupId, funnel);
	}
	
	private void makeProcessGroup(String processGroupId, PositionDTO position, ElementYML ele, Map<String, TemplateYML> templateDB) throws ApiException {
		ProcessGroupEntity pg = new ProcessGroupEntity();
		ProcessGroupDTO dto = new ProcessGroupDTO();
		pg.setComponent(dto);
		pg.setRevision(getRevision());
		pg.setId(ele.id);
		
		dto.setPosition(position);
		dto.setName(ele.name);
		
		ProcessGroupEntity newProcessGroup = processGroupAPI.createProcessGroup(processGroupId, pg);
		
		// All additional properties must be set after the initial create
		newProcessGroup.getComponent().setComments(ele.comment);
		newProcessGroup = processGroupAPI.updateProcessGroup(newProcessGroup.getId(), newProcessGroup);

		// Fill in the process group with its referenced template after it has been made
		// TODO: you aren't allowed to specify the ID for a process group so we must use the newly assigned one
		//		Research if there is a way to circumvent this so exports are 1 to 1 with imports (possibly versionedComponentId)
		TemplateYML refTemplate = templateDB.get(ele.template);
		importTemplate(newProcessGroup.getId(), refTemplate, templateDB);
	}
	
	private void makeRemoteProcessGroup(String processGroupId, PositionDTO position, ElementYML ele) throws ApiException {
		RemoteProcessGroupEntity rpg = new RemoteProcessGroupEntity();
		RemoteProcessGroupDTO dto = new RemoteProcessGroupDTO();
		rpg.setComponent(dto);
		rpg.setRevision(getRevision());
		rpg.setId(ele.id);
		
		dto.setPosition(position);
		dto.setTargetUris(ele.properties.get(HelperYML.TARGET_URIS));
		
		processGroupAPI.createRemoteProcessGroup(processGroupId, rpg);
	}
	
	private void makeOutputPort(String processGroupId, PositionDTO position, ElementYML ele) throws ApiException {
		PortEntity port = new PortEntity();
		PortDTO dto = new PortDTO();
		port.setComponent(dto);
		port.setRevision(getRevision());
		port.setId(ele.id);
		
		dto.setPosition(position);
		dto.setName(ele.name);
		
		processGroupAPI.createOutputPort(processGroupId, port);
	}
	
	private void makeInputPort(String processGroupId, PositionDTO position, ElementYML ele) throws ApiException {
		PortEntity port = new PortEntity();
		PortDTO dto = new PortDTO();
		port.setComponent(dto);
		port.setRevision(getRevision());
		port.setId(ele.id);
		
		dto.setPosition(position);
		dto.setName(ele.name);
		
		processGroupAPI.createInputPort(processGroupId, port);
	}
	
	private void makeLabel(String processGroupId, PositionDTO position, ElementYML ele) throws ApiException {
		LabelEntity label = new LabelEntity();
		LabelDTO dto = new LabelDTO();
		label.setComponent(dto);
		label.setRevision(getRevision());
		label.setId(ele.id);
		
		dto.setPosition(position);
		dto.setLabel(ele.comment);
		
		processGroupAPI.createLabel(processGroupId, label);
	}

	private void makeProcessor(String processGroupId, ElementYML element, Pair<String,BundleDTO> dependency, PositionDTO position) throws ApiException {
		ProcessorEntity p = new ProcessorEntity();
		ProcessorDTO dto = new ProcessorDTO();
		p.setComponent(dto);
		p.setRevision(getRevision());
		p.setId(element.id);
		
		
		dto.setType(dependency.t1);
		dto.setBundle(dependency.t2);
		dto.setName(element.name);
		dto.setPosition(position);
		
		processGroupAPI.createProcessor(processGroupId, p);
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
		rev.setClientId(getClientId());
		rev.setVersion(0L);
		return rev;
	}
	
	// Tester main method
	public static void main(String[] args) {
		BaseCommand.configureApiClients("localhost", "8080", false);
		
		ClearCommand clear = new ClearCommand();
		clear.run();
		
		ImportCommand importCmd = new ImportCommand("./examples/simple/");
		importCmd.run();
	}
}
