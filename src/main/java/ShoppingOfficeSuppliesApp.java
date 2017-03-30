import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

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
        } finally {
            connection.close();
        }
    }

    public static void createTables(Admin admin) throws IOException {

        byte[] tName = Bytes.toBytes("tname");
        byte[] CF1 = Bytes.toBytes("cf1");
        byte[] CF2 = Bytes.toBytes("cf2");

        List<byte[]> colFamilyList = new ArrayList<byte[]>();
        colFamilyList.add(CF1);
        colFamilyList.add(CF2);

        InitTables.createTable(admin, tName, colFamilyList, 3);

    }

}
