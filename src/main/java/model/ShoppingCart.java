package model;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by raghumyneni on 3/31/17.
 */
public class ShoppingCart {

    private String cartId;
    private long pens;
    private long books;
    private long erasers;
    private long markers;

    public ShoppingCart(String cartId, long pens, long books, long erasers, long markers) {
        this.cartId = cartId;
        this.pens = pens;
        this.books = books;
        this.erasers = erasers;
        this.markers = markers;
    }

    public ShoppingCart(byte[] cartId, byte[] pens, byte[] books, byte[] erasers, byte[] markers) {
        this(Bytes.toString(cartId), Bytes.toLong(pens), Bytes.toLong(books),
                Bytes.toLong(erasers), Bytes.toLong(markers));
    }

    public String getCartId() {
        return cartId;
    }

    public void setCartId(String cartId) {
        this.cartId = cartId;
    }

    public long getPens() {
        return pens;
    }

    public void setPens(long pens) {
        this.pens = pens;
    }

    public long getBooks() {
        return books;
    }

    public void setBooks(long books) {
        this.books = books;
    }

    public long getErasers() {
        return erasers;
    }

    public void setErasers(long erasers) {
        this.erasers = erasers;
    }

    public long getMarkers() {
        return markers;
    }

    public void setMarkers(long markers) {
        this.markers = markers;
    }

    @Override
    public String toString() {
        return "ShoppingCart [" +
                "RowKey='" + cartId +
                ", pens=" + pens +
                ", books=" + books +
                ", erasers=" + erasers +
                ", markers=" + markers +
                ']';
    }
}
