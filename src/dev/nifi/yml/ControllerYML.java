package dev.nifi.yml;

import java.util.Map;
import java.util.TreeMap;

import org.apache.nifi.api.toolkit.model.ControllerServiceDTO;
import org.apache.nifi.api.toolkit.model.ControllerServiceEntity;
import org.apache.nifi.api.toolkit.model.PropertyDescriptorDTO;

public class ControllerYML {

	public String name;
	
	public String type;
	
	public String id;
	
	public String comment;
	
	public final Map<String, Object> properties = new TreeMap<String, Object>();
	
	/**
	 * Only for deserialization
	 */
	public ControllerYML() {}
	
	public ControllerYML(ControllerServiceEntity controller, String dependencyReference) {
		ControllerServiceDTO dto = controller.getComponent();
		
		this.id = dto.getId();
		this.name = dto.getName();
		if (!this.name.equals(dependencyReference)) {
			this.type = dependencyReference;
		}
		
		this.comment = controller.getComponent().getComments();

		Map<String, PropertyDescriptorDTO> defaultProperties = dto.getDescriptors();
		Map<String, String> configuredValues = dto.getProperties();
		for (String propertyName : defaultProperties.keySet()) {
			PropertyDescriptorDTO defaultProperty = defaultProperties.get(propertyName);
			String configuredValue = configuredValues.get(propertyName);
			
			// Check if the configuredValue differs from the default value
			if ((defaultProperty.getDefaultValue() == null && configuredValue != null) || 
				(defaultProperty.getDefaultValue() != null && !defaultProperty.getDefaultValue().equals(configuredValue))) {
				this.properties.put(propertyName, configuredValue);
			}
		}
	}
	
	public String getType() {
		if (this.type == null) {
			return this.name;
		}
		return this.type;
	}

}
