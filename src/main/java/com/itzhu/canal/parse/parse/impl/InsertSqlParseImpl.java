package com.itzhu.canal.parse.parse.impl;

import com.itzhu.canal.parse.bean.CanalBean;
import com.itzhu.canal.parse.parse.SqlParse;
import com.itzhu.canal.parse.util.SqlUtil;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * insert sql parse
 */
@Service
public class InsertSqlParseImpl implements SqlParse {
    @Override
    public List<String> parse(CanalBean canalBean) {
        // 拼接 insert into table(filed,fiels)
        StringBuilder sqlBuffer = new StringBuilder("insert into ");
        sqlBuffer.append(canalBean.getDatabase())
                .append(".")
                .append(canalBean.getTable())
                .append("(");
        Map<String, Object> firstDataMap = canalBean.getData().get(0);
        List<String> fields = new ArrayList<>();
        for (String key : firstDataMap.keySet()) {
            sqlBuffer.append("`")
                    .append(key)
                    .append("`")
                    .append(",");
            fields.add(key);
        }
        sqlBuffer.replace(sqlBuffer.lastIndexOf(","), sqlBuffer.lastIndexOf(",") + 1, ") ");
        sqlBuffer.append(" values ");
        // 拼接value
        for (int i = 0; i < canalBean.getData().size(); i++) {
            StringBuilder stringBuilder = new StringBuilder("(");
            for (String field : fields) {
                stringBuilder.append(SqlUtil.transCharStr(canalBean.getData().get(i).get(field),
                            canalBean.getMysqlType().get(field)))
                        .append(",");
            }
            stringBuilder.replace(stringBuilder.lastIndexOf(","), stringBuilder.lastIndexOf(",") + 1, "),");
            sqlBuffer.append(stringBuilder);
        }
        sqlBuffer.replace(sqlBuffer.lastIndexOf(","), sqlBuffer.lastIndexOf(",") + 1, "");
        return Collections.singletonList(sqlBuffer.toString());
    }
}
