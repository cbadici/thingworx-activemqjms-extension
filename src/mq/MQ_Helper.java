package mq;


/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.joda.time.DateTime;

import com.thingworx.data.util.InfoTableInstanceFactory;
import com.thingworx.types.InfoTable;
import com.thingworx.types.collections.ValueCollection;
import com.thingworx.types.primitives.DatetimePrimitive;
import com.thingworx.types.primitives.StringPrimitive;

/**
 * 
 * @author vrosu
 */
public class MQ_Helper {

	//private String url = ActiveMQConnection.DEFAULT_BROKER_URL;

	private Connection connection;
	private String str_Queue_Name;
	private String str_Queue_Address;
	private String str_User;
	private String str_Password;
	private boolean bool_AuthenticationEnabled;



	public MQ_Helper(String str_QName, String str_QAddress, String str_User, String str_Password) {
		str_Queue_Name = str_QName;
		str_Queue_Address = str_QAddress;
		this.str_User = str_User;
		this.str_Password = str_Password;
		if (str_User != null)
		this.bool_AuthenticationEnabled = true;
		else
		this.bool_AuthenticationEnabled = false;
	}
	
	public MQ_Helper(String str_QName, String str_QAddress)
	{
		this(str_QName,str_QAddress, null, null);
	}

	public void Disconnect() throws JMSException {
		if (connection != null)
			connection.close();
		connection = null;
	
		
	}

	public void Connect() throws Exception {
		ConnectionFactory connectionFactory = null;
		if (bool_AuthenticationEnabled)
		connectionFactory = new ActiveMQConnectionFactory(str_User,str_Password,str_Queue_Address);
		else if (!bool_AuthenticationEnabled)
		connectionFactory = new ActiveMQConnectionFactory(str_Queue_Address);
			
		if (connection == null) {
			connection = connectionFactory.createConnection();
			connection.start();
			
		}
	}
	
	public String Send_Message(String str_Message) throws Exception {
		try {
			Connect();
			Session session = connection.createSession(false,
					Session.AUTO_ACKNOWLEDGE);
			Destination destination = session.createQueue(str_Queue_Name);
			MessageProducer producer = session.createProducer(destination);
			if (str_Message == null) {
				str_Message = "No message.";
			}
			TextMessage message = session.createTextMessage(str_Message);
			producer.send(message);
			Disconnect();
			return "Message sent!";
		} catch (JMSException ex) {
			return ex.getMessage();
		}


	}

	@SuppressWarnings("rawtypes")
	public InfoTable GetLastMessages(Integer int_MaxNoOfMessages) {
		try {
			Connect();
			//Queue Browser has no concept of ack-ing a message so whatever we set here it doesn't matter			
			Session session = connection.createSession(false,
					com.sun.messaging.jms.Session.AUTO_ACKNOWLEDGE);
			
			
			//get or create the needed queue
			Queue mq = session.createQueue(str_Queue_Name);
			
			
			QueueBrowser mq_browser = session.createBrowser(mq);
			QueueBrowser mq_browser2=session.createBrowser(mq);


			InfoTable infotbl2 = InfoTableInstanceFactory
					.createInfoTableFromDataShape("ReceivedMessagesShape");
			
			ValueCollection vc = null;
			Enumeration mq_enum = mq_browser.getEnumeration();
			Enumeration mq_enum2=mq_browser2.getEnumeration();
			int int_MessageCount = 0,int_CurrentIndex=0;
			List<String> lst_MessageContent = new ArrayList<String>();
			List<DateTime> lst_MessageDate = new ArrayList<DateTime>();
			if (mq_enum != null) {
				//first we determine the number of messages existing in the queue
				while (mq_enum.hasMoreElements()) {
					++int_MessageCount;
					mq_enum.nextElement();
				}
				
				while (mq_enum2.hasMoreElements())
				{
					++int_CurrentIndex;
					TextMessage message = (TextMessage) mq_enum2.nextElement();
					if (int_CurrentIndex>int_MessageCount-int_MaxNoOfMessages)
					{
						lst_MessageContent.add(message.getText());
						lst_MessageDate.add(new DateTime(message.getJMSTimestamp()));
						
					}
				}
				int int_MessageListLength = lst_MessageContent.size();
//				for (int j = int_MessageListLength - 1; j >= int_MessageListLength - int_MaxNoOfMessages; j--) {
//					
//					vc = new ValueCollection();
//
//					vc.put("Message", new StringPrimitive(lst_MessageContent.get(j)));
//					vc.put("Date", new DatetimePrimitive(lst_MessageDate.get(j)));
//
//					infotbl2.addRow(vc);
//				}
				
				for (int j = 0; j < int_MessageListLength ; j++) {
					
					vc = new ValueCollection();

					vc.put("Message", new StringPrimitive(lst_MessageContent.get(j)));
					vc.put("Date", new DatetimePrimitive(lst_MessageDate.get(j)));

					infotbl2.addRow(vc);
				}
				
				
			}
			session.close();
			Disconnect();
			return infotbl2;

		} catch (Exception e)

		{
			try{
			InfoTable infotbl2 = InfoTableInstanceFactory
					.createInfoTableFromDataShape("ReceivedMessagesShape");
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			
			
			ValueCollection vc = new ValueCollection();

			vc.put("Message", new StringPrimitive(errors.toString()));
			vc.put("Date", new DatetimePrimitive(DateTime.now()));

			infotbl2.addRow(vc);
			return infotbl2;
			}
			catch (Throwable ex)
			{return null;}
			
		}
	}

	public void ReceiveMessage(MessageListener ml,boolean bool_AutoACK) throws Throwable {
		// Getting JMS connection from the server
		
			Connect();

			// Creating session for reading messages
			Session session;
			if (bool_AutoACK)
			session = connection.createSession(false,
					com.sun.messaging.jms.Session.AUTO_ACKNOWLEDGE);
			else 
			session = connection.createSession(false,
						com.sun.messaging.jms.Session.CLIENT_ACKNOWLEDGE);

			Destination destination = session.createQueue(str_Queue_Name);

			// MessageConsumer is used for receiving (consuming) messages
			MessageConsumer consumer = session.createConsumer(destination);

			consumer.setMessageListener(ml);
			
	}
}
