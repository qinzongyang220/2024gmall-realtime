package com.bw.gmallapi.pojo;

import lombok.Data;

import java.io.Serializable;
@Data
public class TT<T> implements Serializable {
    private T columns;
    private T rows;

    public TT(T columns, T rows) {
        this.columns = columns;
        this.rows = rows;
    }
}
