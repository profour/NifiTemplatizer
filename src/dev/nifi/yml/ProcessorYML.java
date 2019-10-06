package dev.nifi.yml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.api.toolkit.model.ConnectionEntity;
import org.apache.nifi.api.toolkit.model.FunnelEntity;
import org.apache.nifi.api.toolkit.model.PortEntity;
import org.apache.nifi.api.toolkit.model.PositionDTO;
import org.apache.nifi.api.toolkit.model.ProcessGroupEntity;
import org.apache.nifi.api.toolkit.model.ProcessorEntity;
import org.apache.nifi.api.toolkit.model.PropertyDescriptorDTO;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import dev.nifi.xml.Criteria;
import dev.nifi.xml.Rules;

public class ProcessorYML {

	
	/**
	 * Human displayed Name for this processor
	 */
	public String name;
	
	/**
	 * Reference the dependencies for valid type names.
	 * 
	 * Reserved keywords include: Input, Output, ProcessGroup
	 */
	public String type;
	
	/**
	 * UUID for this processor (not necessary/optional)
	 */
	public String id;
	
	/**
	 * Template definition to load for this ProcessorGroup
	 */
	public String template;
	
	/**
	 * Visual position of the processor on the canvas
	 */
	public String position;
	
	/**
	 * Any comments/labels that have been applied to this component
	 */
	public String comment;
	
	/**
	 * Only store the list of properties that are different from default
	 */
	public Map<String, String> properties;
	
	/**
	 * Advanced rules based on triggers that conditionally generate attributes
	 */
	public List<RulesYML> rules;
	
	/**
	 * Policy of how FlowFiles are handled when multiple rules are triggered
	 */
	public String rulesPolicy;
	
	/**
	 * Scheduling details for this Processor
	 */
	public Map<String, String> scheduling;
	
	/**
	 * All incoming connections to this Processor
	 */
	public List<InputConnectionYML> inputs;
	
	
	public ProcessorYML(ProcessorEntity pg, String dependencyReference, List<ConnectionEntity> inputs) {
		// Get the unique identifiers for this processor
		this.id = pg.getId();
		this.name = pg.getComponent().getName();
		
		if (!this.name.equals(dependencyReference)) {
			this.type = dependencyReference;
		}
				
		// Populate the list of properties that have changed
		this.properties = new HashMap<String, String>();
		Map<String, PropertyDescriptorDTO> defaultProperties = pg.getComponent().getConfig().getDescriptors();
		Map<String, String> configuredValues = pg.getComponent().getConfig().getProperties();
		for (String propertyName : defaultProperties.keySet()) {
			PropertyDescriptorDTO defaultProperty = defaultProperties.get(propertyName);
			String configuredValue = configuredValues.get(propertyName);
			
			// Check if the configuredValue differs from the default value
			if ((defaultProperty.getDefaultValue() == null && configuredValue != null) || 
				(defaultProperty.getDefaultValue() != null && !defaultProperty.getDefaultValue().equals(configuredValue))) {
				this.properties.put(propertyName, configuredValue);
				System.out.println("Configured Value: " + propertyName + " = " + configuredValue);
			}
		}
		
		// Check to see if there are any advanced annotations on this processor
		String xmlAnnotations = pg.getComponent().getConfig().getAnnotationData();
		if (xmlAnnotations != null && !xmlAnnotations.isEmpty()) {
			this.rules = new ArrayList<>();
			
			XmlMapper xmlMapper = new XmlMapper();
		    try {
				Criteria criteria = xmlMapper.readValue(xmlAnnotations, Criteria.class);
				
				this.rulesPolicy = criteria.flowFilePolicy;
				
				for (Rules rule : criteria.rules) {
					RulesYML r = new RulesYML(rule.conditions, rule.actions);
					
					this.rules.add(r);
				}
				
			} catch (IOException e) {
				// TODO: setup proper logging
				System.out.println("Failed to parse xml: " + xmlAnnotations);
				e.printStackTrace();
			}
		}
		
		// Extract Scheduling details that may have changed for this processor
		// TODO:

		handlePosition(pg.getPosition());
		handleConnections(inputs);
	}


	public ProcessorYML(FunnelEntity f, List<ConnectionEntity> inputs) {
		this.id = f.getId();
		this.type = ReservedComponents.FUNNEL.name();

		handlePosition(f.getPosition());
		handleConnections(inputs);
	}


	public ProcessorYML(ProcessGroupEntity pg, String templateName, List<ConnectionEntity> inputs) {
		this.id = pg.getId();
		this.type = ReservedComponents.PROCESS_GROUP.name();
		this.name = pg.getComponent().getName();
		this.template = templateName + ".yaml";
		this.comment = pg.getComponent().getComments();
		
		handlePosition(pg.getPosition());
		handleConnections(inputs);
	}
	
	public ProcessorYML(PortEntity port, List<ConnectionEntity> inputs) {
		this.id = port.getId();
		this.type = port.getPortType();
		this.name = port.getComponent().getName();

		handlePosition(port.getPosition());
		handleConnections(inputs);
	}


	private void handleConnections(List<ConnectionEntity> inputs) {

		// Process incoming connections to fill out the input listing
		this.inputs = new ArrayList<>();
		if (inputs != null) {
			for (ConnectionEntity connection : inputs) {
				this.inputs.add(new InputConnectionYML(connection));
			}
		}
	}
	
	private void handlePosition(PositionDTO position) {
		// Truncate position down to integer values
		this.position = String.format("%.0f,%.0f", position.getX(), position.getY());
	}

}
