package ar.gov.santafe.simpledb;

import ar.gov.santafe.meduc.dto.SimpleDto;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

/**
 *
 * @author enorrmann
 */
public class SimpleDtoMapper implements ResultSetMapper<SimpleDto> {

    @Override
    public SimpleDto map(int index, ResultSet rs, StatementContext ctx) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        SimpleDto result = new SimpleDto();
        for (int i = 1; i <= columnsNumber; i++) {
            String value = rs.getString(i);
            String columnName = rsmd.getColumnName(i);
            result.add(format(columnName), value);
        }
        return result;
    }

    private String format(String columnName) {
        return columnName == null ? null : columnName.toLowerCase();
    }
}
