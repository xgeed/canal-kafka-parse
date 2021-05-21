package com.itzhu.canal.parse.parse;

import com.itzhu.canal.parse.bean.CanalBean;
import com.itzhu.canal.parse.parse.impl.DdlSqlParseImpl;
import com.itzhu.canal.parse.parse.impl.DeleteSqlParseImpl;
import com.itzhu.canal.parse.parse.impl.InsertSqlParseImpl;
import com.itzhu.canal.parse.parse.impl.UpdateSqlParseImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class SqlParseFactory implements SqlParse{

    public static String INSERT_TYPE = "INSERT";
    public static String UPDATE_TYPE = "UPDATE";
    public static String DELETE_TYPE = "DELETE";

    @Autowired
    InsertSqlParseImpl insertSqlParse;

    @Autowired
    UpdateSqlParseImpl updateSqlParse;

    @Autowired
    DeleteSqlParseImpl deleteSqlParse;

    @Autowired
    DdlSqlParseImpl ddlSqlParse;


    @Override
    public List<String> parse(CanalBean canalBean) {
        if (canalBean.isDdl()) {
            return ddlSqlParse.parse(canalBean);
        }
        if (INSERT_TYPE.equals(canalBean.getType().toUpperCase())) {
            return insertSqlParse.parse(canalBean);
        }
        if (UPDATE_TYPE.equals(canalBean.getType().toUpperCase())) {
            return updateSqlParse.parse(canalBean);
        }
        if (DELETE_TYPE.equals(canalBean.getType().toUpperCase())) {
            return deleteSqlParse.parse(canalBean);
        }
        return null;
    }
}
