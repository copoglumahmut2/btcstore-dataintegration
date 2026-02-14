package com.btc_store.dataintegration.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum MergeStrategyEnum {

    MERGE("MERGE"), OVERRIDE("OVERRIDE");

    private String value;
}
