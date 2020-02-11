package mq;


import java.io.PrintWriter;
import java.io.StringWriter;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.joda.time.DateTime;
import org.slf4j.Logger;

import com.thingworx.data.util.InfoTableInstanceFactory;
import com.thingworx.logging.LogUtilities;
import com.thingworx.metadata.annotations.ThingworxConfigurationTableDefinitions;
import com.thingworx.metadata.annotations.ThingworxEventDefinition;
import com.thingworx.metadata.annotations.ThingworxEventDefinitions;
import com.thingworx.metadata.annotations.ThingworxPropertyDefinition;
import com.thingworx.metadata.annotations.ThingworxPropertyDefinitions;
import com.thingworx.metadata.annotations.ThingworxServiceDefinition;
import com.thingworx.metadata.annotations.ThingworxServiceParameter;
import com.thingworx.metadata.annotations.ThingworxServiceResult;
import com.thingworx.security.context.SecurityContext;
import com.thingworx.things.Thing;
import com.thingworx.things.connected.IConnectedDevice;
import com.thingworx.things.events.ThingworxEvent;
import com.thingworx.types.InfoTable;
import com.thingworx.types.collections.ValueCollection;
import com.thingworx.types.primitives.IntegerPrimitive;
import com.thingworx.types.primitives.StringPrimitive;
import com.thingworx.webservices.context.ThreadLocalContext;

@ThingworxEventDefinitions(events = { @ThingworxEventDefinition(name = "MessageReceived", description = "Message received event", dataShape = "MessageReceivedEvent") })
@ThingworxConfigurationTableDefinitions(tables = { @com.thingworx.metadata.annotations.ThingworxConfigurationTableDefinition(name = "MQConnectionInfo", description = "MQ Broker Connection Parameters", isMultiRow = false, ordinal = 0, dataShape = @com.thingworx.metadata.annotations.ThingworxDataShapeDefinition(fields = {
		@com.thingworx.metadata.annotations.ThingworxFieldDefinition(ordinal = 0, name = "QueueName", description = "MQ queue name", baseType = "STRING", aspects = {
				"defaultValue:MyQueue", "friendlyName:MQ Queue name" }),
		@com.thingworx.metadata.annotations.ThingworxFieldDefinition(ordinal = 1, name = "BrokerAddress", description = "MQ broker address and port", baseType = "STRING", aspects = {
				"defaultValue:tcp://127.0.0.1:61616",
				"friendlyName:MQ broker address and port" }),
		@com.thingworx.metadata.annotations.ThingworxFieldDefinition(ordinal = 2, name = "User", description = "Username", baseType = "STRING", aspects = {
				"defaultValue:",
				"friendlyName:MQ User" }),
		@com.thingworx.metadata.annotations.ThingworxFieldDefinition(ordinal = 3, name = "Password", description = "Password", baseType = "PASSWORD", aspects = {
				"defaultValue:", "friendlyName:MQ Password" }),
		@com.thingworx.metadata.annotations.ThingworxFieldDefinition(ordinal = 4, name = "AuthenticationEnabled", description = "Activate authentication for broker", baseType = "BOOLEAN", aspects = {
				"defaultValue:true",
				"friendlyName:Authenticate using User and Password?" }),
		@com.thingworx.metadata.annotations.ThingworxFieldDefinition(ordinal = 5, name = "ConnectionTimeout", description = "Connection timeout for the broker (miliseconds)", baseType = "INTEGER", aspects = {
				"defaultValue:2000",
				"friendlyName:Connection timeout for the broker, expressed in miliseconds" })

})) })
@ThingworxPropertyDefinitions(properties = {
		@ThingworxPropertyDefinition(name = "EventMaxCount", description = "", category = "", baseType = "INTEGER", isLocalOnly = false, aspects = {
				"defaultValue:50", "dataChangeType:Value" }), @ThingworxPropertyDefinition(name = "CurrentEventCount", description = "", category = "", baseType = "INTEGER", isLocalOnly = false, aspects = {
				"dataChangeType:Value","defaultValue:0" }), @ThingworxPropertyDefinition(name = "BufferCheckInterval", description = "", category = "", baseType = "INTEGER", isLocalOnly = false, aspects = {
						"defaultValue:1000", "dataChangeType:Value" }) })
public class ActiveMQ_JMS_Thing_Listener extends Thing implements MessageListener,
		IConnectedDevice {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3373905042274763702L;

	protected static Logger _logger = LogUtilities.getInstance()
			.getApplicationLogger(ActiveMQ_JMS_Thing_Listener.class);
	public MQ_Helper mqh;
	private String str_Queue_Name;
	private String str_Queue_Address;
	private String str_User;
	private String str_Password;
	private boolean bool_UsingAuthentication = true;
	
	private Integer int_ConnectionTimeout = null;
	private Integer int_Buffer_Check_Interval = null;
	
	int int_MaxNrOfEvents;

	protected void initializeThing() throws Exception {
		str_Queue_Name = ((String) getConfigurationSetting(
				"MQConnectionInfo", "QueueName"));
		
		str_Queue_Address = ((String) getConfigurationSetting(
				"MQConnectionInfo", "BrokerAddress"));
		
		str_User = ((String) getConfigurationSetting(
				"MQConnectionInfo", "User"));

		str_Password = ((String)getConfigurationSetting(
				"MQConnectionInfo", "Password"));

		bool_UsingAuthentication = ((boolean) getConfigurationSetting(
				"MQConnectionInfo", "AuthenticationEnabled"));
		
		int_MaxNrOfEvents = ((IntegerPrimitive)this.getPropertyValue("EventMaxCount")).getValue();
		int_Buffer_Check_Interval = ((IntegerPrimitive)this.getPropertyValue("BufferCheckInterval")).getValue();
		

		int_ConnectionTimeout = new Integer((int) getConfigurationSetting("MQConnectionInfo", "ConnectionTimeout"));
		
		
	}

	protected void startThing() throws Exception {
		if (mqh == null) {
			if (int_ConnectionTimeout != null) {
				str_Queue_Address += "?connectionTimeout="
						+ int_ConnectionTimeout.intValue();
			}
			if (bool_UsingAuthentication) {
				mqh = new MQ_Helper(str_Queue_Name, str_Queue_Address,
						str_User, str_Password);
			} else {
				mqh = new MQ_Helper(str_Queue_Name, str_Queue_Address);
			}
		}
	}

	@ThingworxServiceDefinition(name = "Bind", description = "Binds this Thing as a listener to the specific MQ")
	@ThingworxServiceResult(name = "Result", description = "Result", baseType = "STRING")
	public String Bind(@ThingworxServiceParameter(name = "AutoAcknowledge", description = "", baseType = "BOOLEAN", aspects = {
			"defaultValue:false"}) Boolean bool_AutoACK) {
		try {
			if (mqh != null) {
				mqh.Connect();
				mqh.ReceiveMessage(this,bool_AutoACK.booleanValue());
				return "Success";
			} else
				return "Not initialized!";

		} catch (Throwable er) {
			StringWriter errors = new StringWriter();
			er.printStackTrace(new PrintWriter(errors));
			return errors.toString();
		}

	}

	@ThingworxServiceDefinition(name = "Unbind", description = "Unbinds this listener from the specific MQ")
	@ThingworxServiceResult(name = "Result", description = "Result", baseType = "STRING")
	public String Unbind() {
		try {
			if (mqh != null) {
				mqh.Disconnect();
				;
				return "Disconnect successfull.";
			} else
				return "Not initialized!";

		} catch (Throwable er) {
			StringWriter errors = new StringWriter();
			er.printStackTrace(new PrintWriter(errors));
			return errors.toString();
		}

	}

	protected void stopThing() throws Exception {
		if (mqh != null)
			mqh.Disconnect();
	}

	private void fireSalutationSentEvent(String str) throws Exception {
		ThreadLocalContext.setSecurityContext(SecurityContext
				.createSuperUserContext());
		int CurrentEventCount = ((IntegerPrimitive)this.getPropertyValue("CurrentEventCount")).getValue();
		
		
		while (CurrentEventCount >= int_MaxNrOfEvents)
		{
			Thread.sleep(int_Buffer_Check_Interval);
			 CurrentEventCount = ((IntegerPrimitive)this.getPropertyValue("CurrentEventCount")).getValue();
		}
		this.setPropertyValue("CurrentEventCount", new IntegerPrimitive(++CurrentEventCount));
		ThingworxEvent event = new ThingworxEvent();
		event.setTraceActive(ThreadLocalContext.isTraceActive());
		event.setSecurityContext(ThreadLocalContext.getSecurityContext());
		event.setSource(getName());
		event.setEventName("MessageReceived");
		// the name parameter isn't really used		
		InfoTable data = InfoTableInstanceFactory.createInfoTableFromDataShape("MessageReceivedEvent");
		ValueCollection values = new ValueCollection();
		
		
		values.put("messageReceived", new StringPrimitive(str));
		data.addRow(values);
		event.setEventData(data);
		this.dispatchBackgroundEvent(event);
		
		
		
		ThreadLocalContext.clearSecurityContext();
	}

	@Override
	public void onMessage(Message message) {
		if (message instanceof TextMessage) {
			TextMessage textMessage = (TextMessage) message;
			try {
				
				fireSalutationSentEvent(textMessage.getText());
				
			} catch (Exception e) {

				e.printStackTrace();
			}
		}
	}

	@Override
	public DateTime getLastConnectionTime() {
		return DateTime.now();
	}

	@Override
	public boolean isConnected() {
		if (mqh != null)
			return true;
		else
			return false;
	}
}
