package DAO;

import model.ShoppingCart;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by raghumyneni on 3/31/17.
 */
public class ShoppingCartDAO {

    public static final byte[] TABLE_NAME = Bytes.toBytes("shopping_cart");
    public static final byte[] ITEMS_CF = Bytes.toBytes("items");
    public static final byte[] PENS_COL = Bytes.toBytes("pens");
    public static final byte[] BOOKS_COL = Bytes.toBytes("books");
    public static final byte[] ERASERS_COL = Bytes.toBytes("erasers");
    public static final byte[] MARKERS_COL = Bytes.toBytes("markers");

    private Table table = null;

    public ShoppingCartDAO (Table table) {
        this.table = table;
    }

    public ShoppingCartDAO(Connection connection) throws IOException {
        try {
            this.table = connection.getTable(TableName.valueOf(TABLE_NAME));
        } finally {
            connection.close();
        }
    }

    public Get mkGet(String cartId) {
        Get get = new Get(Bytes.toBytes(cartId));
        get.addFamily(ITEMS_CF);
        return get;
    }

    public Put mkPut(ShoppingCart shoppingCart) {
        Put put = new Put(Bytes.toBytes(shoppingCart.getCartId()));
        put.addColumn(ITEMS_CF, PENS_COL, Bytes.toBytes(shoppingCart.getPens()));
        put.addColumn(ITEMS_CF, BOOKS_COL, Bytes.toBytes(shoppingCart.getBooks()));
        put.addColumn(ITEMS_CF, ERASERS_COL, Bytes.toBytes(shoppingCart.getErasers()));
        put.addColumn(ITEMS_CF, MARKERS_COL, Bytes.toBytes(shoppingCart.getMarkers()));
        return put;
    }

    public Scan mkScan() {
        Scan scan = new Scan();
        scan.addFamily(ITEMS_CF);
        return scan;
    }

    public Delete mkDelete(String cartId) {
        Delete delete = new Delete(Bytes.toBytes(cartId));
        return delete;
    }

    public void close() throws IOException{
        if(this.table != null) {
            this.table.close();
        }
    }

    public ShoppingCart getShoppingCart (String cartId) throws IOException {
        Get get = mkGet(cartId);
        Result result = null;
        try{
            result = this.table.get(get);
            if(result.isEmpty()) {
                return null;
            }
        } finally {
            close();
        }
        return  createShoppingCart(result);
    }

    public void addShoppingCart(String cartId, long pens, long books, long erasers, long markers) throws IOException {
        Put put = mkPut(new ShoppingCart(cartId, pens, books, erasers, markers));
        try {
            this.table.put(put);
        } finally {
            close();
        }
    }

    public void deleteShoppingCart(String cartId) throws IOException {
        Delete delete = mkDelete(cartId);
        try {
            this.table.delete(delete);
        } finally {
            close();
        }
    }

    public List<ShoppingCart> getShoppingCarts() throws IOException {
        List<ShoppingCart> shoppingCarts = new ArrayList<ShoppingCart>();
        Scan scan = mkScan();
        ResultScanner results = null;
        try {
            results = this.table.getScanner(scan);
            for(Result result: results) {
                shoppingCarts.add(createShoppingCart(result));
            }
        } finally {
            results.close();
            close();
        }
        return shoppingCarts;
    }

    public ShoppingCart createShoppingCart(Result result) {
        return new ShoppingCart(result.getRow(),
                result.getValue(ShoppingCartDAO.ITEMS_CF, ShoppingCartDAO.PENS_COL),
                result.getValue(ShoppingCartDAO.ITEMS_CF, ShoppingCartDAO.BOOKS_COL),
                result.getValue(ShoppingCartDAO.ITEMS_CF, ShoppingCartDAO.ERASERS_COL),
                result.getValue(ShoppingCartDAO.ITEMS_CF, ShoppingCartDAO.MARKERS_COL));
    }


}
