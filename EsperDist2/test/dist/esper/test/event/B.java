package dist.esper.test.event;

public class B implements Event{
	public int id;
    public String name;
    public double price;
    public long clientId;
    public int[] ids;
    public String[] names;
    public double[] prices;
    public long[] clientIds;

    public B(int id, String name, double price) {
    	this.id = id;
        this.name = name;
        this.price = price;
    }
    
    public String getType(){
    	return "  B";
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
    
    public long getClientId() {
		return clientId;
	}

	public void setClientId(int clientId) {
		this.clientId = clientId;
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

	public String toString(){
    	return String.format("(B:%d,%s,%f)",id,name,price);
    }
}