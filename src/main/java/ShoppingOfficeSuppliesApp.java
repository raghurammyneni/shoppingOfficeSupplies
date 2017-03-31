import DAO.InventoryDAO;
import model.Inventory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by raghumyneni on 3/30/17.
 */
public class ShoppingOfficeSuppliesApp {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        try {
            createTables(admin);
            InventoryDAO inventoryDAO = new InventoryDAO(connection
                    .getTable(TableName.valueOf(InventoryDAO.TABLE_NAME)));
            inventoryDAO.addInventory("pen", 10);
            inventoryDAO.addInventory("book", 19);
            inventoryDAO.addInventory("marker", 22);
            inventoryDAO.addInventory("eraser", 5);

            List<Inventory> inventories = inventoryDAO.getInventories();

            for(Inventory inventory: inventories) {
                System.out.println(inventory.toString());
            }

        } finally {
            connection.close();
        }
    }

    public static void createTables(Admin admin) throws IOException {

        List<byte[]> colFamilyList = new ArrayList<byte[]>();

        colFamilyList.add(InventoryDAO.STOCK_CF);
        InitTables.createTable(admin, InventoryDAO.TABLE_NAME, colFamilyList, 3);

    }

}
