package ru.atc.camel.opsm.events;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultPollingEndpoint;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;

@UriEndpoint(scheme="opsm", title="OPSM", syntax="opsm://operationPath", consumerOnly=true, consumerClass=OPSMConsumer.class, label="restna")
public class OPSMEndpoint extends DefaultPollingEndpoint {

	public OPSMEndpoint(String uri, String operationPath, OPSMComponent component) {
		super(uri, component);
		this.operationPath = operationPath;
	}
	
	private String operationPath;

	@UriParam
	private OPSMConfiguration configuration;

	public Producer createProducer() throws Exception {
		throw new UnsupportedOperationException("OVMMProducer is not implemented");
	}

	@Override
	public Consumer createConsumer(Processor processor) throws Exception {
		OPSMConsumer consumer = new OPSMConsumer(this, processor);
        return consumer;
	}

	public boolean isSingleton() {
		return true;
	}

	public String getOperationPath() {
		return operationPath;
	}

	public void setOperationPath(String operationPath) {
		this.operationPath = operationPath;
	}

	public OPSMConfiguration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(OPSMConfiguration configuration) {
		this.configuration = configuration;
	}
	
}