package com.btc_store.dataintegration.service;

import org.apache.commons.lang3.tuple.Pair;

import java.io.File;

public interface FileImportService {

    Pair<Boolean,String> importFile(File file, boolean isMoveFile);
}
