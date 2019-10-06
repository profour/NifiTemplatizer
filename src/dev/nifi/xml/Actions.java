package dev.nifi.xml;

public class Actions {

	public String id;

	public String attribute;
	public String value;
	
	public String toString() {
		return attribute + " = " + value;
	}
}
