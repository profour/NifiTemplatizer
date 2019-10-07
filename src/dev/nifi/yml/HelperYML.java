package dev.nifi.yml;

public class HelperYML {
	// Enumeration of statically available types for elements on the workspace
	public enum ReservedComponents {
		INPUT_PORT,
		OUTPUT_PORT,
		PROCESS_GROUP, 
		FUNNEL, 
		LABEL
	}
	
	// font-size
	public static String DEFAULT_STYLE_FONT_SIZE = "12px";
	
	// background-color
	public static String DEFAULT_STYLE_COLOR = "#fff7d7";

	// Constants for style names
	public static final String WIDTH  = "width";
	public static final String HEIGHT = "height";
	public static final String BG_COLOR = "background-color";
	public static final String FONT_SIZE = "font-size";
}