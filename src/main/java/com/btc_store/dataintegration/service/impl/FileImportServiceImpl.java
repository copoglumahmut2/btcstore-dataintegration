package com.btc_store.dataintegration.service.impl;

import com.btc_store.dataintegration.constant.DataIntegrationConstant;
import com.btc_store.dataintegration.service.FileImportService;
import com.btc_store.dataintegration.service.ImportService;
import com.btc_store.dataintegration.service.MediaImportService;
import com.btc_store.domain.enums.DataIntegrationStatus;
import com.btc_store.domain.enums.ImportProcessType;
import com.btc_store.domain.model.custom.SiteModel;
import com.btc_store.domain.model.custom.dataintegration.DataIntegrationLogModel;
import com.btc_store.security.constant.AuthorizationConstants;
import com.btc_store.service.ModelService;
import com.btc_store.service.SiteService;
import com.btc_store.service.constant.ServiceConstant;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.opencsv.CSVReaderHeaderAware;
import com.opencsv.exceptions.CsvValidationException;
import constant.PackageConstant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import util.StoreClassUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static com.btc_store.dataintegration.constant.DataIntegrationConstant.SITE_FIELD;
import static com.btc_store.dataintegration.constant.DataIntegrationConstant.SITE_MODEL;

@Service
@RequiredArgsConstructor
@Slf4j
public class FileImportServiceImpl implements FileImportService {

    private static final String FILE_DATE_PATTERN = "ddMMyyyyHHmmssSSS";
    private static final String UNDERSCORE = "_";
    private static final String CSV_INTEGRATION_USER = "csv_integration_user";

    @Value(value = "${csv.listener.success.folder.path}")
    private String successFolderPath;

    @Value(value = "${csv.listener.error.folder.path}")
    private String errorFolderPath;

    @Value(value = "${csv.listener.processing.folder.path}")
    private String processingFolder;


    protected final ImportService importService;
    protected final MediaImportService mediaImportService;
    protected final ModelService modelService;
    protected final SiteService siteService;

    @Override
    public synchronized Pair<Boolean, String> importFile(File file, boolean isMoveFile) {
        Pair<Boolean, String> isSuccessPair = new ImmutablePair<>(Boolean.FALSE, "ERROR");
        var dataintegrationLogModel = modelService.create(DataIntegrationLogModel.class);
        boolean isSiteModel = false;
        boolean isValid = false;
        try {
            var processTypeIndex = 0;
            var itemTypeIndex = 1;
            if (isMoveFile) {
                log.info("Moving processing folder");
                var processingFolderFilePath = StringUtils.join(processingFolder, LocalDateTime.now().format(DateTimeFormatter.ofPattern(FILE_DATE_PATTERN))
                        + UNDERSCORE + file.getName());
                FileUtils.moveFile(file, new File(FilenameUtils.separatorsToSystem(processingFolderFilePath)),
                        StandardCopyOption.COPY_ATTRIBUTES);

                file = new File(processingFolderFilePath);
                processTypeIndex++;
                itemTypeIndex++;
            }

            log.info("Started to importing file : " + file.getName());
            SiteModel siteModel;
            var itemAndProcessType = StringUtils.split(FilenameUtils.getBaseName(file.getName()), UNDERSCORE);
            final var processType = StringUtils.capitalize(itemAndProcessType[processTypeIndex]);//Save
            final var typeName = StringUtils.capitalize(itemAndProcessType[itemTypeIndex]);//Category

            Authentication authentication = new UsernamePasswordAuthenticationToken(CSV_INTEGRATION_USER, null,
                    AuthorityUtils.createAuthorityList(AuthorizationConstants.SUPER_ADMIN));
            SecurityContextHolder.getContext().setAuthentication(authentication);

            ImportProcessType importProcessType;
            if (StringUtils.equalsIgnoreCase(processType, ImportProcessType.SAVE.getValue())) {
                importProcessType = ImportProcessType.SAVE;
            } else if (StringUtils.equalsIgnoreCase(processType, ImportProcessType.REMOVE.getValue())) {
                importProcessType = ImportProcessType.REMOVE;
            } else if (StringUtils.equalsIgnoreCase(processType, ImportProcessType.FILE.getValue())) {
                importProcessType = ImportProcessType.FILE;
            } else {
                throw new NoSuchMethodException("Process type must be Save,Remove or File");
            }

            var className = StoreClassUtils.generateClassName(typeName, ServiceConstant.HYPHEN, PackageConstant.MODEL_PREFIX);
            Class tclass = StoreClassUtils.getClassForPackage(className, PackageConstant.DOMAIN_PACKAGE);
            try (var reader = new CSVReaderHeaderAware(new InputStreamReader(new BOMInputStream(new FileInputStream(file)), StandardCharsets.UTF_8))) {

                List<Map<String, String>> allRows = new ArrayList<>();
                Map<String, String> row;
                while ((row = reader.readMap()) != null) {
                    //csv dosyasından gelen csv-comment-line kolonu ignore edilir.
                    row.remove("csv-comment-line");
                    allRows.add(row);
                }


                isSiteModel = StringUtils.equals(StoreClassUtils.getSimpleName(tclass), SITE_MODEL);
                //check site
                if (BooleanUtils.isFalse(isSiteModel) &&
                        CollectionUtils.isNotEmpty(allRows)) {
                    var siteCode = allRows.get(0).get(DataIntegrationConstant.SITE_FIELD);

                    if (StringUtils.isEmpty(siteCode)) {
                        throw new CsvValidationException(SITE_FIELD.concat(" field must not be null"));
                    }

                    var allSiteCodes = allRows.stream().map(a -> a.get(DataIntegrationConstant.SITE_FIELD)).collect(Collectors.toList());

                    var allMatch = false;
                    if (!Iterables.all(allSiteCodes, Predicates.isNull())) {
                        allMatch = allSiteCodes.stream().allMatch(s -> s.equals(siteCode));
                        isValid = true;
                    }

                    siteModel = siteService.getSiteModel(siteCode);
                    if (BooleanUtils.isFalse(allMatch)) {
                        dataintegrationLogModel.setStatus(DataIntegrationStatus.FAIL);
                        dataintegrationLogModel.setCount(allRows.size());
                        dataintegrationLogModel.setSite(siteModel);
                        dataintegrationLogModel.setCode(UUID.randomUUID().toString());
                        dataintegrationLogModel.setImportProcessType(importProcessType);
                        dataintegrationLogModel.setStartDate(new Date());
                        dataintegrationLogModel.setEndDate(new Date());
                        dataintegrationLogModel.setItemType(className);
                        dataintegrationLogModel.setDescription("Site field must be same for all rows");
                        modelService.save(dataintegrationLogModel);
                        throw new CsvValidationException("Site field must be same for all rows");
                    }

                    dataintegrationLogModel.setStatus(DataIntegrationStatus.PROCESSING);
                    dataintegrationLogModel.setCount(allRows.size());
                    dataintegrationLogModel.setSite(siteModel);
                    dataintegrationLogModel.setCode(UUID.randomUUID().toString());
                    dataintegrationLogModel.setImportProcessType(importProcessType);
                    dataintegrationLogModel.setStartDate(new Date());
                    dataintegrationLogModel.setItemType(className);
                    modelService.save(dataintegrationLogModel);

                    if (Objects.equals(ImportProcessType.FILE, importProcessType)) {
                        isSuccessPair = mediaImportService.importData(tclass, isMoveFile, allRows);
                    } else {
                        isSuccessPair = importService.importData(tclass, allRows, importProcessType, siteModel);
                    }

                } else if (BooleanUtils.isTrue(isSiteModel) && CollectionUtils.isNotEmpty(allRows)) {
                    if (Objects.equals(ImportProcessType.FILE, importProcessType)) {
                        isSuccessPair = mediaImportService.importData(tclass, isMoveFile, allRows);
                    } else {
                        try {
                            siteModel = siteService.getSiteModel(allRows.get(0).get(DataIntegrationConstant.CODE_FIELD));
                        } catch (Exception e) {
                            siteModel = null;
                        }

                        isSuccessPair = importService.importData(tclass, allRows, importProcessType, siteModel);
                    }
                }

            }


        } catch (Exception e) {
            isSuccessPair = new ImmutablePair<>(Boolean.FALSE, ExceptionUtils.getMessage(e));
            log.error("Error occurred while {} file importing .... {}", file.getName(), e.getMessage());
        }


        //According to result move file success or error...
        try {
            if (BooleanUtils.isTrue(isSuccessPair.getKey())) {
                if (BooleanUtils.isFalse(isSiteModel)) {
                    dataintegrationLogModel.setEndDate(new Date());
                    dataintegrationLogModel.setStatus(DataIntegrationStatus.SUCCESS);
                    dataintegrationLogModel.setDescription(DataIntegrationStatus.SUCCESS.toString());
                    modelService.save(dataintegrationLogModel);
                }
                if (BooleanUtils.isTrue(isMoveFile)) {
                    var successFolder = StringUtils.join(successFolderPath, file.getName());
                    FileUtils.moveFile(file, new File(FilenameUtils.separatorsToSystem(successFolder)),
                            StandardCopyOption.REPLACE_EXISTING);
                    if (BooleanUtils.isFalse(isSiteModel)) {
                        dataintegrationLogModel.setLogFile(successFolder);
                        modelService.save(dataintegrationLogModel);
                    }

                }
            } else {

                var logFilePath = StringUtils.join(errorFolderPath,
                        FilenameUtils.getBaseName(file.getName()), ".log");
                Path logFile = Path.of(logFilePath);
                if (BooleanUtils.isFalse(isSiteModel) && BooleanUtils.isTrue(isValid)) {
                    dataintegrationLogModel.setEndDate(new Date());
                    dataintegrationLogModel.setDescription(isSuccessPair.getValue());
                    dataintegrationLogModel.setLogFile(logFile.toString());
                    dataintegrationLogModel.setStatus(DataIntegrationStatus.FAIL);
                    modelService.save(dataintegrationLogModel);
                }

                if (BooleanUtils.isTrue(isMoveFile)) {
                    var errorFolder = StringUtils.join(errorFolderPath, file.getName());

                    FileUtils.moveFile(file, new File(FilenameUtils.separatorsToSystem(errorFolder)),
                            StandardCopyOption.REPLACE_EXISTING);
                    Files.writeString(logFile, isSuccessPair.getValue());
                }
            }


        } catch (Exception e) {
            log.error("Error occurred while moving/writing the error file to error folder..." + e.getMessage());
        }

        return isSuccessPair;
    }
}
