package dev.nifi.yml;

import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.api.toolkit.model.ControllerServiceDTO;
import org.apache.nifi.api.toolkit.model.ControllerServiceEntity;
import org.apache.nifi.api.toolkit.model.PropertyDescriptorDTO;

public class ControllerYML {

	public String id;
	
	public String name;
	
	public Map<String, Object> properties;
	
	public ControllerYML(ControllerServiceEntity controller) {
		ControllerServiceDTO dto = controller.getComponent();
		
		this.id = dto.getId();
		this.name = dto.getName();

		this.properties = new HashMap<String, Object>();
		Map<String, PropertyDescriptorDTO> defaultProperties = dto.getDescriptors();
		Map<String, String> configuredValues = dto.getProperties();
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
	}

}
