import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.List;

/**
 * Created by raghumyneni on 3/30/17.
 */
public class InitTables {

    public static void deleteTable(Admin admin, byte[] tableName) throws IOException {
        if(admin.tableExists(TableName.valueOf(tableName))) {
            if(admin.isTableEnabled(TableName.valueOf(tableName))) {
                admin.disableTable(TableName.valueOf(tableName));
            }
            admin.deleteTable(TableName.valueOf(tableName));
        }
    }

    public static void createTable (Admin admin, byte[] tableName,
                                    List<byte[]> colFamilies, int maxVersions) throws IOException {

        if(admin.tableExists(TableName.valueOf(tableName))) {
            deleteTable(admin, tableName);
        }

        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        for(byte[] columnFamily: colFamilies) {
            table.addFamily(new HColumnDescriptor(columnFamily).setMaxVersions(maxVersions));
        }

        admin.createTable(table);

    }

}
