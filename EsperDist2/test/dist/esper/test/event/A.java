package dist.esper.test.event;

public class A implements Event{
	public int id;
    public String name;
    public double price;
    public int time;
    public int[] ids;

    public A(int id, String name, double price) {
    	this.id = id;
        this.name = name;
        this.price = price;
    }
    
    public String getType(){
    	return "A";
    }

    public int getId(){
    	return id;
    }
    
    public int[] getIds() {
		return ids;
	}

	public void setIds(int[] ids) {
		this.ids = ids;
	}

	public void setId(int id) {
		this.id = id;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	public String getName() {
        return name;
    }

    public double getPrice() {
        return price;
    }
    
    public int getTime() {
		return time;
	}

	public void setTime(int time) {
		this.time = time;
	}

	public String toString(){
    	return String.format("(A:%d,%s,%f)",id,name,price);
    }
}