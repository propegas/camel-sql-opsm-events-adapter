package ru.atc.camel.opsm.events;

import java.util.Map;

import org.apache.camel.Endpoint;
import org.apache.camel.impl.UriEndpointComponent;

public class OPSMComponent extends UriEndpointComponent {

	public OPSMComponent() {
		super(OPSMEndpoint.class);
	}

	@Override
	protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
		
		OPSMEndpoint endpoint = new OPSMEndpoint(uri, remaining, this);		
		OPSMConfiguration configuration = new OPSMConfiguration();
		
		// use the built-in setProperties method to clean the camel parameters map
		setProperties(configuration, parameters);
		
		endpoint.setConfiguration(configuration);		
		return endpoint;
	}
}