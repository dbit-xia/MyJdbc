package node;
import java.util.List;

/**
 * @author yululiang
 * @version 1.0
 * @date 2019/11/12 12:28
 */
public class SqliteTable {

    private int commitFrequency = 300;
    private String querySourceDataSql;
    private String targetTableName;
    private List<String> targetFields;
    private String createTargetTableSql;

    public int getCommitFrequency() {
        return commitFrequency;
    }

    public void setCommitFrequency(int commitFrequency) {
        this.commitFrequency = commitFrequency;
    }

    public List<String> getTargetFields() {
        return targetFields;
    }

    public void setCreateTargetTableSql(String createTargetTableSql) {
        this.createTargetTableSql = createTargetTableSql;
    }

    public String getCreateTargetTableSql() {
        return createTargetTableSql;
    }

    public void setQuerySourceDataSql(String querySourceDataSql) {
        this.querySourceDataSql = querySourceDataSql;
    }

    public String getQuerySourceDataSql() {
        return querySourceDataSql;
    }

    public void setTargetFields(List<String> targetFields) {
        this.targetFields = targetFields;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }
}

