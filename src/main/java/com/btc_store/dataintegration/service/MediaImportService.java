package com.btc_store.dataintegration.service;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;

public interface MediaImportService {

    Pair<Boolean, String> importData(Class itemType, boolean move, List<Map<String, String>> data);
}
