package ar.gov.santafe.simpledb;

import ar.gov.santafe.meduc.dto.SimpleDto;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.Update;

/**
 *
 * @author enorrmann
 */
public class SimpleDbAccess {

    private final DataSource dataSource;
    
    public SimpleDbAccess(DataSource dataSource){
        this.dataSource = dataSource;
    }

    private void bind(Update updateQuery, SimpleDto simpleDto) {
        Set<String> keys = simpleDto.getAtributos().keySet();
        for (String aKey : keys) {
            if (simpleDto.get(aKey) != null) {
                updateQuery.bind(aKey.toUpperCase(), simpleDto.get(aKey));
            }
        }
    }

    private void bind(Query query, SimpleDto searchDto) {
        Set<String> keys = searchDto.getAtributos().keySet();
        for (String aKey : keys) {
            if (searchDto.get(aKey) != null) {
                query.bind(aKey.toUpperCase(), searchDto.get(aKey));
            }
        }
    }

    private String buildUpdate(String table, SimpleDto simpleDto) {
        StringBuilder queryBuilder = new StringBuilder();
        final String comma = " , ";
        final String equals = "=:";
        String idField = inferIdFieldName(table);
        queryBuilder.append(" update ").append(table).append(" set ");
        Iterator<String> keys = notNullAttributes(simpleDto);
        while (keys.hasNext()) {
            String aKey = keys.next();
            queryBuilder.append(aKey).append(equals).append(aKey);
            if (keys.hasNext()) {
                queryBuilder.append(comma);
            }
        }
        queryBuilder.append(" where ");
        queryBuilder.append(idField);
        queryBuilder.append(equals);
        queryBuilder.append(idField);
        String queryString = queryBuilder.toString();
        return queryString.toUpperCase();
    }

    private String buildSelect(String table, SimpleDto simpleDto) {
        StringBuilder queryBuilder = new StringBuilder();
        final String and = " and ";
        final String equals = "=:";
        queryBuilder.append(" select * from ").append(table);
        queryBuilder.append(" where ");
        Iterator<String> keys = simpleDto.getAtributos().keySet().iterator();
        while (keys.hasNext()) {
            String aKey = keys.next();
            queryBuilder.append(aKey).append(equals).append(aKey);
            if (keys.hasNext()) {
                queryBuilder.append(and);
            }
        }
        String queryString = queryBuilder.toString();
        return queryString.toUpperCase();
    }

    private String buildInsert(String table, SimpleDto simpleDto) {
        final String comma = " , ";
        final String semicolon = ":";
        StringBuilder queryBuilder = new StringBuilder();
        StringBuilder params = new StringBuilder();
        String idField = inferIdFieldName(table);
        String sequenceName = inferSequenceName(table);
        params.append(sequenceName);
        params.append(comma);
        queryBuilder.append(" insert into ").append(table);
        queryBuilder.append(" ( ");
        queryBuilder.append(idField);
        queryBuilder.append(comma);
        Iterator<String> keys = notNullAttributes(simpleDto);
        while (keys.hasNext()) {
            String aKey = keys.next();
            queryBuilder.append(aKey);
            params.append(semicolon).append(aKey);
            if (keys.hasNext()) {
                queryBuilder.append(comma);
                params.append(comma);
            }
        }
        queryBuilder.append(" ) values ( ");
        queryBuilder.append(params);
        queryBuilder.append(" ) ");
        String queryString = queryBuilder.toString();
        return queryString.toUpperCase();
    }

    private Handle getHandle() {
        DBI dbi = new DBI(dataSource);
        Handle handle = dbi.open();
        return handle;
    }

    public SimpleDto update(SimpleDto simpleDto, String table) {
        Handle handle = getHandle();
        String queryString = buildUpdate(table, simpleDto);
        Update updateQuery = handle.createStatement(queryString);
        bind(updateQuery, simpleDto);
        updateQuery.execute();
        handle.close();
        return simpleDto;

    }

    public List<SimpleDto> search(SimpleDto searchDto, String table) {
        Handle handle = getHandle();
        String queryString = buildSelect(table, searchDto);
        Query<Map<String, Object>> query = handle.createQuery(queryString);
        Query<SimpleDto> blas = query.map(new SimpleDtoMapper());
        List<SimpleDto> resultList = blas.list();
        handle.close();
        return resultList;
    }

    public SimpleDto findById(String id, String table) {
        Handle handle = getHandle();
        SimpleDto searchDto = new SimpleDto();
        searchDto.add(inferIdFieldName(table), id);
        String queryString = buildSelect(table, searchDto);
        Query<Map<String, Object>> query = handle.createQuery(queryString);
        bind(query, searchDto);

        Query<SimpleDto> blas = query.map(new SimpleDtoMapper());
        List<SimpleDto> resultList = blas.list();
        handle.close();
        return resultList.get(0);
    }

    public List<SimpleDto> select(SimpleDto simpleDto, String table) {
        Handle handle = getHandle();
        Query<Map<String, Object>> query = handle.createQuery("select * from " + table);
        Query<SimpleDto> blas = query.map(new SimpleDtoMapper());
        List<SimpleDto> resultList = blas.list();
        handle.close();
        return resultList;

    }

    public SimpleDto insert(SimpleDto simpleDto, String table) {
        Handle handle = getHandle();
        String queryString = buildInsert(table, simpleDto);
        Update updateQuery = handle.createStatement(queryString);
        bind(updateQuery, simpleDto);
        updateQuery.execute();
        handle.close();
        return simpleDto;

    }

    private String inferSequenceName(String tableName) {
        return "SEQ_" + tableName + ".nextval";
    }

    private String inferIdFieldName(String tableName) {
        int index = tableName.indexOf("_") + 1;
        return "ID_" + tableName.substring(index);
    }

    public List<SimpleDto> select(String queryString) {
        Handle handle = getHandle();
        Query<Map<String, Object>> query = handle.createQuery(queryString);
        Query<SimpleDto> blas = query.map(new SimpleDtoMapper());
        List<SimpleDto> resultList = blas.list();
        handle.close();
        return resultList;
    }

    private Iterator<String> notNullAttributes(SimpleDto simpleDto) {
        List<String> notNullKeys = new ArrayList<>();
        Set<String> keys = simpleDto.getAtributos().keySet();
        for (String aKey : keys) {
            if (simpleDto.get(aKey) != null) {
                notNullKeys.add(aKey);
            }
        }
        return notNullKeys.iterator();
    }

}
