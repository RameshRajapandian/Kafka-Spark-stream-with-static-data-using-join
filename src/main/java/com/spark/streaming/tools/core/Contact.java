package com.spark.streaming.tools.core;

import java.io.Serializable;
import java.util.StringTokenizer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Contact  implements Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int contactId;
    private String firstName;
    private String lastName;
    private int sanlimt;

    public Contact(){

    }
    public Contact(int contactId, String firstName, String lastName, int sanlimt) {
        this.contactId = contactId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.sanlimt = sanlimt; 
    }

    public void parseString(String csvStr){
        StringTokenizer st = new StringTokenizer(csvStr,",");
        contactId = Integer.parseInt(st.nextToken());
        firstName = st.nextToken();
        lastName = st.nextToken();
    }


    public int getContactId() {
        return contactId;
    }

    public void setContactId(int contactId) {
        this.contactId = contactId;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }
    
    public int getSanlimt() {
		return sanlimt;
	}
	public void setSanlimt(int sanlimt) {
		this.sanlimt = sanlimt;
	}
	
	@Override
    public String toString() {
        return " Contact{" +
                "contactId=" + contactId +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", sanlimt='" + sanlimt + '\''+
                "'}";
    }

    public static void main(String[] argv)throws Exception{
        ObjectMapper mapper = new ObjectMapper();
        Contact contact = new Contact();
        contact.setContactId(1);
        contact.setFirstName("Sachin");
        contact.setLastName("Tendulkar");
        System.out.println(mapper.writeValueAsString(contact));
        contact.parseString("1,Rahul,Dravid");
        System.out.println(mapper.writeValueAsString(contact));
    }
}