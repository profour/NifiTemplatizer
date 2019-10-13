package dev.nifi.commands;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.api.toolkit.ApiException;
import org.apache.nifi.api.toolkit.api.ProcessGroupsApi;
import org.apache.nifi.api.toolkit.model.BundleDTO;
import org.apache.nifi.api.toolkit.model.ProcessGroupEntity;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import dev.nifi.utils.DependencyBuilder;
import dev.nifi.utils.ObjectBuilder;
import dev.nifi.utils.Pair;
import dev.nifi.yml.ControllerYML;
import dev.nifi.yml.ElementYML;
import dev.nifi.yml.HelperYML;
import dev.nifi.yml.HelperYML.ReservedComponents;
import dev.nifi.yml.InputConnectionYML;
import dev.nifi.yml.TemplateYML;

public class ImportCommand extends BaseCommand {

	private final ProcessGroupsApi processGroupAPI = new ProcessGroupsApi(getApiClient());
	
	private final String importDir;
	
	private ObjectBuilder builder = new ObjectBuilder(getApiClient(), getClientId());
	
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
			List<TemplateYML> templates = HelperYML.load(importDir);

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
			importTemplate("root", templateDB.get("root.yaml"), templateDB);
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
		// Get the real UUID name for "root"
		if (processGroupId.equals("root")) {
			ProcessGroupEntity rootPG = processGroupAPI.getProcessGroup(processGroupId);
			processGroupId = rootPG.getId();
		}
		
		// Setup the process Group id
		builder.enterProcessGroup(processGroupId);
		try {
			// Canonical type name -> (Type, Bundle)
			Map<String, Pair<String, BundleDTO>> depLookup = DependencyBuilder.createDependencyLookup(template.dependencies);
			builder.setDependenciesLookup(depLookup);
			
			// Ensure all process groups are populated first
			createProcessGroups(template, templateDB);
			
			// Create all controller services that may be needed to service processor elements
			createControllerServices(template);
			
			// Create all of the canvas elements
			createElements(template);
			
			// Linkage must run after create elements to ensure all src/dst pairs can be satisfied
			createLinkage(template);
		} finally {
			builder.leaveProcessGroup();
		}
		
	}
	
	private void createProcessGroups(TemplateYML template, Map<String, TemplateYML> templateDB) throws ApiException {
		
		for (ElementYML ele : template.components) {
			// Only deal with Process Groups
			if (HelperYML.isProcessGroup(ele.type)) {
				ReservedComponents type = HelperYML.ReservedComponents.valueOf(ele.getType());
				
				switch (type) {
				case PROCESS_GROUP:
				{
					ProcessGroupEntity pg = builder.makeProcessGroup(ele, templateDB);
					
					// Fill in the contents of the process group we just made
					importTemplate(pg.getId(), templateDB.get(ele.template), templateDB);
					break;
				}
				case REMOTE_PROCESS_GROUP:
				{
					builder.makeRemoteProcessGroup(ele);
					break;
				}
				default:
					// Ignore all non process group types
				}
			}
		}
		
	}
	
	private void createControllerServices(TemplateYML template) throws ApiException {
		for (ControllerYML controller : template.controllers) {
			builder.makeControllerService(controller);
		}
	}
	
	private void createElements(TemplateYML template) throws ApiException {
		
		for (ElementYML ele : template.components) {
			Pair<String, BundleDTO> dep = builder.lookup(ele.getType());
			
			if (dep != null) {
				builder.makeProcessor(ele);
			} else {
				ReservedComponents type = HelperYML.ReservedComponents.valueOf(ele.getType());
				switch (type) {
				case FUNNEL:
				{
					builder.makeFunnel(ele);
					break;
				}
				case INPUT_PORT:
				{
					builder.makeInputPort(ele);
					break;
				}
				case LABEL:
				{
					builder.makeLabel(ele);
					break;
				}
				case OUTPUT_PORT:
				{
					builder.makeOutputPort(ele);
					break;
				}
				default:
					// Ignore the process group stuff that were already handled
				}
			}
		}
	}
	
	private void createLinkage(TemplateYML template) throws ApiException {
		
		Map<String, ElementYML> lookup = new HashMap<String, ElementYML>();
		for (ElementYML element : template.components) {
			lookup.put(element.id, element);
		}
		
		// For all components, connect their inputs
		for (ElementYML destination : template.components) {
			// Skip over any elements that don't have inputs (no lines connecting to them)
			if (destination.inputs == null) {
				continue;
			}

			// Connecting all incoming lines to this element
			for (InputConnectionYML input : destination.inputs) {
				ElementYML source = lookup.get(input.source);
				
				builder.makeConnection(source, destination, input);
			}
		}
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
