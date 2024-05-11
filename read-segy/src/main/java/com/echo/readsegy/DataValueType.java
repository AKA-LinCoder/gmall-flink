package com.echo.readsegy;

public enum DataValueType {
    //4字节IBM浮点数
    IBM_FLOAT(1),
    // 4 字节，两互补整数
    TWO_COMPLEMENT_INT(2),
    //2 字节，两互补整数
    TWO_COMPLEMENT_SHORT(3),
    //4 字节带增益定点数（过时，不再使用）
    GAIN_FIXED_POINT(4),
    //4字节IEEE浮点数
    IEEE_FLOAT(5),
    //现在没有使用
    UNUSED_6(6),
    //现在没有使用
    UNUSED_7(7),
    //1字节，两互补整数
    TWO_COMPLEMENT_BYTE(8);

    private final int value;

    DataValueType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static DataValueType fromValue(int value) {
        for (DataValueType type : DataValueType.values()) {
            if (type.value == value) {
                return type;
            }
        }
        return null;
    }
}