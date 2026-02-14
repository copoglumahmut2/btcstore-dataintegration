package com.btc_store.dataintegration.listener;

import com.btc_store.dataintegration.service.FileImportService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
@Slf4j
public class CsvFileListener {

    @Value(value = "${csv.listener.process.folder.path}")
    private String processFolderPath;

    protected final FileImportService fileImportService;

    @Scheduled(fixedDelay = 10000, initialDelay = 1000)
    public void importCsvFileCron() {
        var directoryName = Path.of(StringUtils.join(processFolderPath)).normalize().toString();

        var files = FileUtils.listFiles(new File(directoryName), new String[]{"csv"}, false);

        //last created file must be read lastly...
        var processFiles = files
                .stream().sorted(Comparator.comparing(File::lastModified)).collect(Collectors.toList());

        processFiles.forEach(processFile -> fileImportService.importFile(processFile, true));

    }
}
