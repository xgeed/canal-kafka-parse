package com.itzhu.canal.parse.parse.impl;

import com.itzhu.canal.parse.bean.CanalBean;
import com.itzhu.canal.parse.parse.SqlParse;
import com.itzhu.canal.parse.util.SqlUtil;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * delete sql parse
 */
@Service
public class DeleteSqlParseImpl implements SqlParse {
    @Override
    public List<String> parse(CanalBean canalBean) {
        List<String> sqlList = new ArrayList<>();
        for (Map<String, Object> data : canalBean.getData()) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("delete from ")
                    .append(canalBean.getDatabase())
                    .append(".")
                    .append(canalBean.getTable())
                    .append(" where ");
            if (canalBean.getPkNames() == null) {
                //無主鍵，按全字段删除
                for (Map.Entry<String, Object> entry : data.entrySet()) {
                    sqlBuilder.append(entry.getKey())
                            .append(entry.getValue() == null ? " is " : "=")
                            .append(SqlUtil.transCharStr(entry.getValue(), canalBean.getMysqlType().get(entry.getKey())))
                            .append(" and ");
                }
            } else {
                // 按主键delete
                for (Map.Entry<String, Object> entry : data.entrySet()) {
                    if (canalBean.getPkNames().contains(entry.getKey())) {
                        sqlBuilder.append(entry.getKey())
                                .append(entry.getValue() == null ? " is " : "=")
                                .append(SqlUtil.transCharStr(entry.getValue(), canalBean.getMysqlType().get(entry.getKey())))
                                .append(" and ");
                    }
                }
            }
            sqlBuilder.replace(sqlBuilder.lastIndexOf("and"), sqlBuilder.lastIndexOf("and") + 3, "");
            sqlList.add(sqlBuilder.toString());
        }
        return sqlList;
    }
}
