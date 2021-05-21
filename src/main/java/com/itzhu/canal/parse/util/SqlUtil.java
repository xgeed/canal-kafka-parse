package com.itzhu.canal.parse.util;

public class SqlUtil {
    public static Object transCharStr(Object val, Object fieldType) {
        if (String.valueOf(fieldType).contains("char")) {
            return "'" + val + "'";
        }
        return val;
    }
}
