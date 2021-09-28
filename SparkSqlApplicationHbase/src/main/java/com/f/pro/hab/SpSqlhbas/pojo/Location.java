/**
 * 
 */
package com.f.pro.hab.SpSqlhbas.pojo;

/**
 * @author cloudera
 *
 */
public class Location {
float[] coordinates;
	
	public Location(float[] coordinates) {
		super();
		this.coordinates = coordinates;
	}

    @Override
    public String toString() {
    	if (coordinates == null) return "";
        return "{coordinates:[" +
                " longitude: " + coordinates[0]  +   
                ", latitude: " + coordinates[1] +
                "]}";
    }
    public Location() {
		// TODO Auto-generated constructor stub
	}
    @Override
    	public boolean equals(Object obj) {
    		// TODO Auto-generated method stub
    		return super.equals(obj);
    	}
    @Override
    	public int hashCode() {
    		// TODO Auto-generated method stub
    		return super.hashCode();
    	}
}
