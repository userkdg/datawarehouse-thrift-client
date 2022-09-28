package com.terry.impala.thrift.hive;

import jline.internal.Log;
import org.apache.hive.jdbc.ClosedOrCancelledStatementException;
import org.apache.hive.jdbc.HiveStatement;
import org.apache.hive.jdbc.Utils;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.thrift.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jarod.Kong
 */
public class QueryInstance {
    private static String host = "192.168.235.52";
    private static int port = 10000;
    private static String username = "hive";
    private static String passsword = "hive";
    private static TTransport transport;
    private static TCLIService.Client client;

    static {
        try {
            transport = QueryTool.getSocketInstance(host, port, username,
                    passsword);
            client = new TCLIService.Client(new TBinaryProtocol(transport));
            transport.open();
        } catch (TTransportException e) {
            Log.info("hive collection error!");
        }
    }

    private TOperationState tOperationState = null;
    private Map<String, Object> resultMap = new HashMap<String, Object>();

    public void submitQuery2(String command) throws Throwable {

        TOperationHandle tOperationHandle;
        TExecuteStatementResp resp = null;

        TSessionHandle sessHandle = QueryTool.openSession(client)
                .getSessionHandle();

        TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle,
                command);
        // 异步运行
        execReq.setRunAsync(false);

        // 执行sql
        resp = client.ExecuteStatement(execReq);// 执行语句

        tOperationHandle = resp.getOperationHandle();// 获取执行的handle

        if (tOperationHandle == null) {
            //语句执行异常时，会把异常信息放在resp.getStatus()中。
            throw new Exception(resp.getStatus().getErrorMessage());
        }

        String queryLog = getQueryLog(tOperationHandle);
        System.out.println(queryLog);
        List<TColumnDesc> columns = getColumns(tOperationHandle);
        for (TColumnDesc column : columns) {
            System.out.println(column);
        }
        List<Object> results = getResults(tOperationHandle);
        for (Object result : results) {
            System.out.println(result);
        }

        queryLog = getQueryLog(tOperationHandle);
        System.out.println(queryLog);
    }

    public String getQueryLog(TOperationHandle tOperationHandle)
            throws Exception {
        List<String> log = getQueryLog(tOperationHandle, true, 1000);
        return String.join("\n", log);
    }

    public TOperationState getQueryHandleStatus(
            TOperationHandle tOperationHandle) throws Exception {

        if (tOperationHandle != null) {
            TGetOperationStatusReq statusReq = new TGetOperationStatusReq(
                    tOperationHandle);
            TGetOperationStatusResp statusResp = client
                    .GetOperationStatus(statusReq);

            tOperationState = statusResp.getOperationState();

        }
        return tOperationState;
    }

    public List<TColumnDesc> getColumns(TOperationHandle tOperationHandle)
            throws Throwable {
        TGetResultSetMetadataResp metadataResp;
        TGetResultSetMetadataReq metadataReq;
        TTableSchema tableSchema;
        metadataReq = new TGetResultSetMetadataReq(tOperationHandle);
        metadataResp = client.GetResultSetMetadata(metadataReq);
        List<TColumnDesc> columnDescs = null;
        tableSchema = metadataResp.getSchema();
        if (tableSchema != null) {
            columnDescs = tableSchema.getColumns();
            return columnDescs;
        }
        return columnDescs;
    }


    private TFetchOrientation getFetchOrientation(boolean incremental) {
        if (incremental) {
            return TFetchOrientation.FETCH_NEXT;
        } else {
            return TFetchOrientation.FETCH_FIRST;
        }
    }
    public List<String> getQueryLog(TOperationHandle stmtHandle, boolean incremental, int fetchSize)
            throws SQLException, ClosedOrCancelledStatementException {

        List<String> logs = new ArrayList<String>();
        TFetchResultsResp tFetchResultsResp = null;

        try {
            if (stmtHandle != null) {
                TFetchResultsReq tFetchResultsReq = new TFetchResultsReq(stmtHandle,
                        getFetchOrientation(incremental), fetchSize);
//                tFetchResultsReq.setFetchType((short)1);
                tFetchResultsResp = client.FetchResults(tFetchResultsReq);
                Utils.verifySuccessWithInfo(tFetchResultsResp.getStatus());
            } else {
                return logs;
            }
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Error when getting query log: " + e, e);
        } finally {
        }

        RowSet rowSet = RowSetFactory.create(tFetchResultsResp.getResults(), TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V5);
        for (Object[] row : rowSet) {
            logs.add((String)row[0]);
        }
        return logs;
    }
    /**
     * 获取执行结果 select语句
     */
    public List<Object> getResults(TOperationHandle tOperationHandle) throws Throwable {
        TFetchResultsReq fetchReq = new TFetchResultsReq();
        fetchReq.setOperationHandle(tOperationHandle);
        fetchReq.setMaxRows(1000);
        TFetchResultsResp re = client.FetchResults(fetchReq);
        List<Object> list_row = new ArrayList<Object>();

        TRowSet results = re.getResults();
        List<TColumn> list = results.getColumns();
        for (TColumn field : list) {
            if (field.isSetStringVal()) {
                list_row.add(field.getStringVal().getValues());
            } else if (field.isSetDoubleVal()) {
                list_row.add(field.getDoubleVal().getValues());
            } else if (field.isSetI16Val()) {
                list_row.add(field.getI16Val().getValues());
            } else if (field.isSetI32Val()) {
                list_row.add(field.getI32Val().getValues());
            } else if (field.isSetI64Val()) {
                list_row.add(field.getI64Val().getValues());
            } else if (field.isSetBoolVal()) {
                list_row.add(field.getBoolVal().getValues());
            } else if (field.isSetByteVal()) {
                list_row.add(field.getByteVal().getValues());
            }
        }
        return list_row;
    }

    public void cancelQuery(TOperationHandle tOperationHandle) throws Throwable {
        if (tOperationState != TOperationState.FINISHED_STATE) {
            TCancelOperationReq cancelOperationReq = new TCancelOperationReq();
            cancelOperationReq.setOperationHandle(tOperationHandle);
            client.CancelOperation(cancelOperationReq);
        }
    }
}

