package com.btc_store.dataintegration.service;

import com.btc_store.domain.enums.ImportProcessType;
import com.btc_store.domain.model.custom.SiteModel;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;

public interface ImportService {

    Pair<Boolean,String> importData(Class itemType, List<Map<String, String>> data, ImportProcessType importProcessType, SiteModel siteModel);
}
