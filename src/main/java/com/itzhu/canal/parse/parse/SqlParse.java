package com.itzhu.canal.parse.parse;

import com.itzhu.canal.parse.bean.CanalBean;

import java.util.List;

public interface SqlParse {

    /**
     * 解析成sql
     * @param canalBean canal message bean
     * @return string sql
     */
    List<String> parse(CanalBean canalBean);
}
