package DAO;

import model.Inventory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by raghumyneni on 3/30/17.
 */
public class InventoryDAO {

    public static final byte[] TABLE_NAME = Bytes.toBytes("inventory");
    public static final byte[] STOCK_CF = Bytes.toBytes("stock");
    public static final byte[] QUANTITY_COL = Bytes.toBytes("quantity");

    private Table table = null;

    public InventoryDAO (Table table) {
        this.table = table;
    }

    public InventoryDAO(Connection connection) throws IOException {
        try {
            this.table = connection.getTable(TableName.valueOf(TABLE_NAME));
        } finally {
            connection.close();
        }
    }

    public Get mkGet(String stock) {
        Get get = new Get(Bytes.toBytes(stock));
        get.addFamily(STOCK_CF);
        return get;
    }

    public Put mkPut(Inventory inventory) {
        Put put = new Put(Bytes.toBytes(inventory.getStock()));
        put.addColumn(STOCK_CF, QUANTITY_COL, Bytes.toBytes(inventory.getQuantity()));
        return put;
    }

    public Scan mkScan() {
        Scan scan = new Scan();
        scan.addFamily(STOCK_CF);
        return scan;
    }

    public Delete mkDelete(String stock) {
        Delete delete = new Delete(Bytes.toBytes(stock));
        return delete;
    }

    public void close() throws IOException{
        if(this.table != null) {
            this.table.close();
        }
    }

    public Inventory getInventory(String stock) throws IOException {
        Get get = mkGet(stock);
        Result result = null;
        try{
            result = this.table.get(get);
            if(result.isEmpty()) {
                return null;
            }
        } finally {
            close();
        }
        return  createInventory(result);
    }

    public void addInventory(String stock, long quantity) throws IOException {
        Put put = mkPut(new Inventory(stock, quantity));
        try {
            this.table.put(put);
        } finally {
            close();
        }
    }

    public void deleteInventory(String stock) throws IOException {
        Delete delete = mkDelete(stock);
        try {
            this.table.delete(delete);
        } finally {
            close();
        }
    }

    public List<Inventory> getInventories() throws IOException {
        List<Inventory> inventories = new ArrayList<Inventory>();
        Scan scan = mkScan();
        ResultScanner results = null;
        try {
            results = this.table.getScanner(scan);
            for(Result result: results) {
                inventories.add(createInventory(result));
            }
        } finally {
            results.close();
            close();
        }
        return inventories;
    }

    public Inventory createInventory (Result result) {
        return new Inventory(result.getRow(), result.getValue(STOCK_CF, QUANTITY_COL));
    }

    public Result checkOutWithIncrement(String stockId, String cartId, long amt) throws IOException {
        Inventory inventory = this.getInventory(stockId);
        long currentQuantity = inventory.getQuantity();
        Result result = null;
        try {
            if (currentQuantity - amt < 0) {
                System.out.println("Not enough inventory for " + stockId);
            } else {
                Increment inc = new Increment(Bytes.toBytes(stockId));
                inc.addColumn(STOCK_CF, QUANTITY_COL, -amt);
                inc.addColumn(STOCK_CF, Bytes.toBytes(cartId), amt);
                result = table.increment(inc);
            }
        } finally {
            close();
        }
        return result;
    }

}
