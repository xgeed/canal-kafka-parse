package com.itzhu.canal.parse.parse.impl;

import com.itzhu.canal.parse.bean.CanalBean;
import com.itzhu.canal.parse.parse.SqlParse;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

/**
 * ddl sql parse
 */
@Service
public class DdlSqlParseImpl implements SqlParse {
    @Override
    public List<String> parse(CanalBean canalBean) {
        return Collections.singletonList(canalBean.getSql());
    }
}
