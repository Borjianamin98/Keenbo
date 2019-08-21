package in.nimbo.dao.hbase;

import in.nimbo.common.config.HBaseConfig;
import in.nimbo.common.exception.HBaseException;
import in.nimbo.common.utility.LinkUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseDAOImpl implements HBaseDAO {
    private HBaseConfig config;
    private Connection connection;

    public HBaseDAOImpl(Connection connection, HBaseConfig config) {
        this.connection = connection;
        this.config = config;
    }

    public void close() throws IOException {
        connection.close();
    }

    @Override
    public boolean[] contains(List<String> links) {
        try (Table table = connection.getTable(TableName.valueOf(config.getLinksTable()))) {
            List<Get> gets = new ArrayList<>();
            for (String link : links) {
                gets.add(new Get(Bytes.toBytes(LinkUtility.reverseLink(link))));
            }
            return table.existsAll(gets);
        } catch (IOException e) {
            throw new HBaseException(e);
        }
    }
}
