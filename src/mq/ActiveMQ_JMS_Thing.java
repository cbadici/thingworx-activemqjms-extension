package mq;


import org.slf4j.Logger;

import com.thingworx.logging.LogUtilities;
import com.thingworx.metadata.annotations.ThingworxConfigurationTableDefinitions;
import com.thingworx.metadata.annotations.ThingworxServiceDefinition;
import com.thingworx.metadata.annotations.ThingworxServiceParameter;
import com.thingworx.metadata.annotations.ThingworxServiceResult;
import com.thingworx.things.Thing;
import com.thingworx.types.InfoTable;



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
				"defaultValue:true","friendlyName:Authenticate using User and Password?" }),
		@com.thingworx.metadata.annotations.ThingworxFieldDefinition(ordinal = 5, name = "ConnectionTimeout", description = "Connection timeout for the broker (miliseconds)", baseType = "INTEGER", aspects = {
				"defaultValue:2000","friendlyName:Connection timeout for the broker, expressed in miliseconds" })

})) })

public class ActiveMQ_JMS_Thing extends Thing {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6535993542011013839L;

	protected static Logger _logger = LogUtilities.getInstance()
			.getApplicationLogger(ActiveMQ_JMS_Thing.class);
	public MQ_Helper mqh;
	private String str_Queue_Name;
	private String str_Queue_Address;
	private String str_User ;
	private String str_Password;
	private boolean bool_UsingAuthentication = true;
	
	private Integer int_ConnectionTimeout = null;

	protected void initializeThing() throws Exception {
		str_Queue_Name = (String) getConfigurationSetting("MQConnectionInfo", "QueueName");
		str_Queue_Address = (String) getConfigurationSetting("MQConnectionInfo", "BrokerAddress");
		str_User = (String)	getConfigurationSetting("MQConnectionInfo", "User");
		
		str_Password = (String) getConfigurationSetting("MQConnectionInfo", "Password");
		
		bool_UsingAuthentication =(boolean) getConfigurationSetting("MQConnectionInfo",  "AuthenticationEnabled");
		
		int_ConnectionTimeout = new Integer((int) getConfigurationSetting("MQConnectionInfo", "ConnectionTimeout"));
		
		
		
		if (mqh == null)
		{
			if (int_ConnectionTimeout != null)
			{
				str_Queue_Address +="?connectionTimeout="+int_ConnectionTimeout.intValue();
			}
			if (bool_UsingAuthentication)
			{
				mqh = new MQ_Helper(str_Queue_Name, str_Queue_Address,str_User,str_Password);
			}
			else
			{
				mqh = new MQ_Helper(str_Queue_Name, str_Queue_Address);
			}
		}		
	}

	@ThingworxServiceDefinition(name = "GetLastMessages", description = "Get the last messages from MQ")
	@ThingworxServiceResult(name = "Result", description = "Result", baseType = "INFOTABLE", aspects = { "dataShape:ReceivedMessagesShape" })
	public InfoTable GetLastMessages(
			@ThingworxServiceParameter(name = "MaxNumberOfMessages", description = "", baseType = "INTEGER") Integer int_MaxNoOfMessages
			)
			throws Exception {
		
		return mqh.GetLastMessages(int_MaxNoOfMessages);
	}

	@ThingworxServiceDefinition(name = "SendMessage", description = "Send a message to the specified MQ")
	@ThingworxServiceResult(name = "Result", description = "Result", baseType = "STRING")
	public String SendMessage(
			@ThingworxServiceParameter(name = "Message", description = "", baseType = "STRING") String str_Message)
			throws Exception {
		
		return mqh.Send_Message(str_Message);
	}

}
