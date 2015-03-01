package dist.esper.test.event;

public class C implements Event{
	public int id;
    public String name;
    public double price;

    public C(int id, String name, double price) {
    	this.id = id;
        this.name = name;
        this.price = price;
    }
    
    public String getType(){
    	return "    C";
    }

    public int getId(){
    	return id;
    }
    
    public String getName() {
        return name;
    }

    public double getPrice() {
        return price;
    }
    
    public String toString(){
    	return String.format("(C:%d,%s,%f)",id,name,price);
    }
}