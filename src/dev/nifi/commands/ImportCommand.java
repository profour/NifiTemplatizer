package dev.nifi.commands;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.api.toolkit.ApiException;
import org.apache.nifi.api.toolkit.api.ProcessGroupsApi;
import org.apache.nifi.api.toolkit.api.ProcessorsApi;
import org.apache.nifi.api.toolkit.model.BundleDTO;
import org.apache.nifi.api.toolkit.model.PositionDTO;
import org.apache.nifi.api.toolkit.model.ProcessGroupEntity;
import org.apache.nifi.api.toolkit.model.ProcessorEntity;
import org.apache.nifi.api.toolkit.model.RelationshipDTO;

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
	private final ProcessorsApi processorAPI = new ProcessorsApi(getApiClient());
	
	private final String importDir;
	
	private final boolean developerMode;
	
	private ObjectBuilder builder = new ObjectBuilder(getApiClient(), getClientId());
	
	public ImportCommand(final String importDir, boolean devMode) {
		super();
		
		if (importDir == null) {
			this.importDir = ".";
		} else {
			this.importDir = importDir;
		}
		
		this.developerMode = devMode;
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
			
			// Only create this metadata label if reproducible exports are needed (active development)
			// Production Install/Deployments should set this to false unless you expect to make modifications in production (don't do it!)
			if (this.developerMode) {
				// Create a label on the canvas to store useful data for NiFi Templatizer to store state
				createMetadataLabel(template);
			}
		} finally {
			builder.leaveProcessGroup();
		}
		
	}
	
	private void createMetadataLabel(TemplateYML template) throws ApiException {
		PositionDTO position = new PositionDTO();
		position.setX(Double.MIN_VALUE);
		position.setY(Double.MIN_VALUE);
		
		for (ElementYML element : template.components) {
			if (element.position != null) {
				PositionDTO pos = HelperYML.createPosition(element.position);
				
				position.setX(Math.max(position.getX(), pos.getX()));
				position.setY(Math.max(position.getY(), pos.getY()));
			}
		}
		position.setX(position.getX() + 500.0);
		position.setY(position.getY() + 500.0);
		
		ElementYML metadataLabel = new ElementYML();
		metadataLabel.comment = "Imported By NiFi Templatizer\n\nasdf\nsdfjkl\nwkerjl\nkewflkjwle\n";
		metadataLabel.position = HelperYML.formatPosition(position.getX(), position.getY());
		metadataLabel.styles.put(HelperYML.WIDTH, HelperYML.formatDoubleTruncated(175.0));
		metadataLabel.styles.put(HelperYML.HEIGHT, HelperYML.formatDoubleTruncated(20.0));
		metadataLabel.styles.put(HelperYML.BG_COLOR, "#465ff0");
		
		builder.makeLabel(metadataLabel);
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
			
			// If we have a non-null dependency, it means it is a processor and not an intrinsic NiFi type
			if (dep != null) {
				builder.makeProcessor(ele);
			} else {
				ReservedComponents type = ReservedComponents.valueOf(ele.getType().toUpperCase());
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
		
		Map<String, Set<String>> usedRelationships = new HashMap<>();
		
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
				
				// Track which relationships were used
				if (!usedRelationships.containsKey(source.id)) {
					usedRelationships.put(source.id, new HashSet<String>());
				}
				usedRelationships.get(source.id).addAll(input.from);
			}
		}
		
		// Auto Terminate relationships that were unused
		for (ElementYML element : template.components) {
			// Skip past any non-processor types (only processors can have relationships)
			if (HelperYML.isReserved(element.type)) {
				continue;
			}
			
			// Check if this processor has any relationships that have been used
			Set<String> used = usedRelationships.get(element.id);
			if (used == null) {
				used = new HashSet<String>();
			}
			
			// Pull the configuration for the processor as-is in NiFi
			String newId = builder.getNewId(element.id);
			ProcessorEntity processor = processorAPI.getProcessor(newId);
			
			// Search for the unused relationships
			List<String> unused = new ArrayList<String>();
			for (RelationshipDTO relationship : processor.getComponent().getRelationships()) {
				if (!used.contains(relationship.getName())) {
					unused.add(relationship.getName());
				}
			}
			
			// If there are any unused relationships, auto terminate them
			if (!unused.isEmpty()) {
				processor.getComponent().getConfig().setAutoTerminatedRelationships(unused);
				
				processorAPI.updateProcessor(processor.getId(), processor);
			}
		}
	}
	
	// Tester main method
	public static void main(String[] args) {
		BaseCommand.configureApiClients("localhost", "8080", false);
		
		ClearCommand clear = new ClearCommand();
		clear.run();
		
		ImportCommand importCmd = new ImportCommand("./examples/simple/", true);
		importCmd.run();
	}
}
