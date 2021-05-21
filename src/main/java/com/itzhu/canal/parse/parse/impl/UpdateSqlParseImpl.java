package com.itzhu.canal.parse.parse.impl;

import com.itzhu.canal.parse.bean.CanalBean;
import com.itzhu.canal.parse.parse.SqlParse;
import com.itzhu.canal.parse.util.SqlUtil;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * update sql parse
 */
@Service
public class UpdateSqlParseImpl implements SqlParse {

    @Override
    public List<String> parse(CanalBean canalBean) {
        // 1.待更新字段和新值
        List<Map<String, Object>> updateFieldList = new ArrayList<>();
        // 根据old字段种的更新前的值，按需更新data种的新值
        for (int i=0; i < canalBean.getOld().size() ; i++) {
            Map<String, Object> updateFieldMap = new HashMap<>();
            Map<String, Object> oldItem = canalBean.getOld().get(i);
            for (Map.Entry<String, Object> entry : oldItem.entrySet()) {
                updateFieldMap.put(entry.getKey(), canalBean.getData().get(i).get(entry.getKey()));
            }
            updateFieldList.add(updateFieldMap);
        }
        // 2.update过滤条件
        List<Map<String, Object>> updateWhereList = new ArrayList<>();
        if (canalBean.getPkNames() == null || canalBean.getPkNames().isEmpty()) {
            // 2.1 主键不存时，按更新前得所有字段做为where条件
            Map<String, Object> updateWhereMap = new HashMap<>();
            for (int i=0; i < canalBean.getData().size() ; i++) {
                updateWhereMap.putAll(canalBean.getData().get(i));
                updateWhereMap.putAll(canalBean.getOld().get(i));
                updateWhereList.add(updateWhereMap);
            }
        } else {
            // 2.2 主键存在
            for (int i=0; i < canalBean.getData().size() ; i++) {
                Map<String, Object> updateWhereMap = new HashMap<>();
                for (String pkName : canalBean.getPkNames()) {
                    updateWhereMap.put(pkName, canalBean.getData().get(i).get(pkName));
                }
                // 如果主键被更新,需要按原始主键更新
                for (Map.Entry<String, Object> entry : canalBean.getOld().get(i).entrySet()) {
                    if (canalBean.getPkNames().contains(entry.getKey())) {
                        updateWhereMap.put(entry.getKey(), entry.getValue());
                    }
                }
                updateWhereList.add(updateWhereMap);
            }
        }

        // 3.组装sql
        List<String> sqlList = new ArrayList<>();
        for (int i = 0; i < updateFieldList.size(); i++) {
            // set
            StringBuffer sbf = new StringBuffer("update ");
            sbf.append(canalBean.getDatabase()).append(".")
                    .append(canalBean.getTable()).append(" set ");
            for (Map.Entry<String, Object> entry : updateFieldList.get(i).entrySet()) {
                sbf.append(entry.getKey())
                        .append(entry.getValue() == null ? " is " : "=" )
                        .append(SqlUtil.transCharStr(entry.getValue(), canalBean.getMysqlType().get(entry.getKey())))
                        .append(",");
            }
            // where
            sbf.replace(sbf.lastIndexOf(","), sbf.lastIndexOf(",") + 1, " where ");
            for (Map.Entry<String, Object> entry : updateWhereList.get(i).entrySet()) {
                sbf.append(entry.getKey())
                        .append(entry.getValue() == null ? " is " : "=" )
                        .append(SqlUtil.transCharStr(entry.getValue(), canalBean.getMysqlType().get(entry.getKey())))
                        .append(" and ");
            }
            sbf.replace(sbf.lastIndexOf("and"), sbf.lastIndexOf("and") + 3, "");
            sqlList.add(sbf.toString());
        }
        return sqlList;
    }
}
