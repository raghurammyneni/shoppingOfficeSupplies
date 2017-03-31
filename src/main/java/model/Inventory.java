package model;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by raghumyneni on 3/30/17.
 */
public class Inventory {

    private String stock;
    private long quantity;

    public Inventory(String stock, long quantity) {
        this.stock = stock;
        this.quantity = quantity;
    }

    public Inventory(byte[] stock, byte[] quantity) {
        this(Bytes.toString(stock), Bytes.toLong(quantity));
    }

    public String getStock() {
        return stock;
    }

    public void setStock(String stock) {
        this.stock = stock;
    }

    public long getQuantity() {
        return quantity;
    }

    public void setQuantity(long quantity) {
        this.quantity = quantity;
    }

    @Override
    public String toString() {
        return "Inventory [key - " + stock + ", quantity - " + quantity + "]";
    }

}
