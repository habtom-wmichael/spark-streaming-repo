package model;

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

}
