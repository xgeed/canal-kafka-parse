package com.itzhu.canal.parse.bean;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * canal 投递到Kafka后message Bean
 */
@Data
public class CanalBean {
    /**
     * 数据
     */
    private List<Map<String, Object>> data;
    /**
     * 数据库名称
     */
    private String database;
    private long es;
    /**
     * 递增，从1开始
     */
    private int id;
    /**
     * 是否是DDL语句
     */
    private boolean isDdl;
    /**
     * 表结构的字段类型
     */
    private Map<String, Object> mysqlType;
    /**
     * UPDATE语句，旧数据
     */
    private List<Map<String, Object>> old;
    /**
     * 主键名称
     */
    private List<String> pkNames;
    /**
     * sql语句
     */
    private String sql;
    private Map<String, Object> sqlType;
    // 表面
    private String table;
    private long ts;
    /**
     * INSERT、UPDATE、DELETE 等等
     */
    private String type;
}
