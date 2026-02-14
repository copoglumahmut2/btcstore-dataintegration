package com.btc_store.dataintegration.setup;

import com.btc_store.dataintegration.service.FileImportService;
import com.btc_store.domain.enums.ImportProcessType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileFilter;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

@Component
@Slf4j
@RequiredArgsConstructor
public class InitialSetup {

    protected final FileImportService fileImportService;

    @Value("${csv.initial.data.file.path}")
    private String initialDataFilePath;

    @Value("${csv.initial.data.file.status}")
    private boolean status;

    @Value("${csv.initial.data.file.media.status}")
    private boolean mediaStatus;

    @Value("${csv.initial.project.name}")
    private String project;


    @Bean
    @DependsOn("registerAllInterceptors")
    public void initialDataSetup() {
        if (BooleanUtils.isTrue(status)) {
            var initialFolderPath = Path.of(StringUtils.join(initialDataFilePath, project)).normalize().toString();
            log.info("Initial Folder Path => " + initialFolderPath);
            FileFilter fileFilter = file -> FilenameUtils.isExtension(file.getName(), "csv");
            var files = new File(initialFolderPath).listFiles();
            if (Objects.nonNull(files)) {
                var sortedFiles = Arrays.asList(files).stream().sorted().collect(Collectors.toList());
                sortedFiles.stream().forEach(f -> Arrays.asList(f.listFiles(fileFilter))
                        .forEach(csvFile -> {
                            if (StringUtils.startsWith(csvFile.getName(), ImportProcessType.FILE.getValue())) {
                                if (mediaStatus) {
                                    fileImportService.importFile(csvFile, false);
                                }
                            } else {
                                fileImportService.importFile(csvFile, false);
                            }
                        }));
            }
        }


    }
}
