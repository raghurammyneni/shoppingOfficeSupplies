import DAO.InventoryDAO;
import DAO.ShoppingCartDAO;
import model.Inventory;
import model.ShoppingCart;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;

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

        InventoryDAO inventoryDAO = new InventoryDAO(connection
                .getTable(TableName.valueOf(InventoryDAO.TABLE_NAME)));

        ShoppingCartDAO shoppingCartDAO = new ShoppingCartDAO(connection
                .getTable(TableName.valueOf(ShoppingCartDAO.TABLE_NAME)));

        try {

            if (args.length > 0 && (args[0].contains("init"))||(args[0].equalsIgnoreCase("setup"))) {
                createTables(admin);

                saveInventoryTableData(inventoryDAO);
                printInventoryTable(inventoryDAO);

                saveShoppingCartTableData(shoppingCartDAO);
                printShoppingCartTable(shoppingCartDAO);
            } else if(args.length > 2 && args[0].equalsIgnoreCase("delete")) {
                if(args[1].equalsIgnoreCase("inventory")) {
                    deleteInventory(inventoryDAO, args[2]);
                } else if (args[1].equalsIgnoreCase("shopping_cart")) {
                    System.out.println("Calling delete shopping cart");
                    deleteShoppingCart(shoppingCartDAO, args[2]);
                }
            } else if (args.length > 1 && args[0].equalsIgnoreCase("checkout")) {
                // TODO 2b run checkout
                checkout(inventoryDAO, shoppingCartDAO, args[1]);
            }

        } finally {
            connection.close();
        }
    }

    public static void createTables(Admin admin) throws IOException {

        List<byte[]> colFamilyList = new ArrayList<byte[]>();

        colFamilyList.add(InventoryDAO.STOCK_CF);
        InitTables.createTable(admin, InventoryDAO.TABLE_NAME, colFamilyList, 3);
        colFamilyList.clear();

        colFamilyList.add(ShoppingCartDAO.ITEMS_CF);
        InitTables.createTable(admin, ShoppingCartDAO.TABLE_NAME, colFamilyList, 3);
        colFamilyList.clear();
    }

    public static void saveInventoryTableData(InventoryDAO inventoryDAO) throws IOException {
        System.out.println(" Inserting rows in Inventory Table: ");
        inventoryDAO.addInventory("pen", 10);
        inventoryDAO.addInventory("book", 19);
        inventoryDAO.addInventory("marker", 22);
        inventoryDAO.addInventory("eraser", 5);
    }

    public static void deleteInventory(InventoryDAO inventoryDAO, String stock) throws IOException {
        inventoryDAO.deleteInventory(stock);
        System.out.println("Inventory deleted - " + stock);
    }

    public static void printInventoryTable(InventoryDAO inventoryDAO) throws IOException {
        List<Inventory> inventories = inventoryDAO.getInventories();
        for(Inventory inventory: inventories) {
            System.out.println(inventory.toString());
        }
    }

    public static void saveShoppingCartTableData(ShoppingCartDAO shoppingCartDAO) throws IOException {
        System.out.println(" Inserting rows in ShoppingCart Table: ");
        shoppingCartDAO.addShoppingCart("Ben", 1, 2, 2, 1);
        shoppingCartDAO.addShoppingCart("John", 1, 2, 2, 1);
        shoppingCartDAO.addShoppingCart("Lisa", 1, 2, 2, 1);
        shoppingCartDAO.addShoppingCart("Bee", 1, 2, 2, 1);
    }

    public static void deleteShoppingCart(ShoppingCartDAO shoppingCartDAO, String cartId) throws IOException {
        shoppingCartDAO.deleteShoppingCart(cartId);
        System.out.println("Cart deleted - " + cartId);
    }

    public static void printShoppingCartTable(ShoppingCartDAO shoppingCartDAO) throws IOException {
        List<ShoppingCart> shoppingCarts = shoppingCartDAO.getShoppingCarts();
        for(ShoppingCart shoppingCart: shoppingCarts) {
            System.out.println(shoppingCart.toString());
        }
    }

    public static void checkout(InventoryDAO inventoryDAO, ShoppingCartDAO shoppingCartDAO,
                                String cartId) throws IOException {
        ShoppingCart cart = shoppingCartDAO.getShoppingCart(cartId);
        checkout(inventoryDAO, cart);

        shoppingCartDAO.deleteShoppingCart(cartId);
    }

    public static void checkout(InventoryDAO inventoryDAO, ShoppingCart cart) throws IOException {
        printInventoryTable(inventoryDAO);

        Result result = inventoryDAO.checkOutWithIncrement("pen", cart.getCartId(), cart.getPens());
        result = inventoryDAO.checkOutWithIncrement("book", cart.getCartId(), cart.getBooks());
        result = inventoryDAO.checkOutWithIncrement("eraser", cart.getCartId(), cart.getErasers());
        result = inventoryDAO.checkOutWithIncrement("marker", cart.getCartId(), cart.getMarkers());

        System.out.println("Checkout Done");

        printInventoryTable(inventoryDAO);
    }

}
