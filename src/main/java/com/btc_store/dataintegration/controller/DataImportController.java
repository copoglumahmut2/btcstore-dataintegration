package com.btc_store.dataintegration.controller;

import com.btc_store.dataintegration.constant.DataIntegrationConstant;
import com.btc_store.dataintegration.service.ImportService;
import com.btc_store.dataintegration.service.MediaImportService;
import com.btc_store.domain.data.custom.restservice.ServiceResponseData;
import com.btc_store.domain.enums.DataIntegrationStatus;
import com.btc_store.domain.enums.ImportProcessType;
import com.btc_store.domain.enums.ProcessStatus;
import com.btc_store.domain.model.custom.SiteModel;
import com.btc_store.domain.model.custom.dataintegration.DataIntegrationLogModel;
import com.btc_store.domain.model.custom.extend.ItemModel;
import com.btc_store.service.ModelService;
import com.btc_store.service.SiteService;
import com.btc_store.service.constant.ServiceConstant;
import com.btc_store.service.exception.StoreRuntimeException;
import com.btc_store.service.exception.model.ModelReadException;
import com.btc_store.service.user.UserService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.exceptions.CsvValidationException;
import constant.PackageConstant;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.context.MessageSource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import util.StoreClassUtils;
import util.Messages;

import java.util.*;
import java.util.stream.Collectors;

import static com.btc_store.dataintegration.constant.DataIntegrationConstant.SITE_MODEL;

@RestController
@RequestMapping(value = "/dataimport")
@RequiredArgsConstructor
@Slf4j
public class DataImportController {

    protected final ImportService importService;
    protected final MediaImportService mediaImportService;
    protected final ModelService modelService;
    protected final SiteService siteService;
    protected final UserService userService;
    protected final MessageSource messageSource;


    @GetMapping(DataIntegrationConstant.HEART_BEAT)
    public ResponseEntity<String> heartBeat() {
        return ResponseEntity.ok().build();
    }

    @SneakyThrows
    @PostMapping("/{processType}/{itemType}")
    public ServiceResponseData processData(@PathVariable String processType, @PathVariable String itemType, @RequestBody String body) {
        var response = new ServiceResponseData();
        response.setStatus(ProcessStatus.ERROR);
        var className = StoreClassUtils.generateClassName(itemType, ServiceConstant.HYPHEN, PackageConstant.MODEL_PREFIX);
        Class itemClass = StoreClassUtils.getClassForPackage(className, PackageConstant.DOMAIN_PACKAGE);

        //check permission...
        checkPermission(itemClass, StringUtils.capitalize(StringUtils.lowerCase(processType)));

        log.info("Processing started.Process : {} Type : {}", processType, itemType);
        Pair<Boolean, String> isSuccessPair = new ImmutablePair<>(Boolean.FALSE, "ERROR");
        var isSiteModel = false;
        SiteModel siteModel = null;
        var dataintegrationLogModel = modelService.create(DataIntegrationLogModel.class);
        dataintegrationLogModel.setRequestJson(body);
        dataintegrationLogModel.setCode(UUID.randomUUID().toString());
        try {
            ObjectMapper mapper = new ObjectMapper();
            List<Map<String, String>> allRows = mapper.readValue(body, new TypeReference<>() {
            });

            isSiteModel = StringUtils.equals(StoreClassUtils.getSimpleName(itemClass), SITE_MODEL);

            ImportProcessType importProcessType;
            if (StringUtils.equalsIgnoreCase(processType, ImportProcessType.SAVE.getValue())) {
                importProcessType = ImportProcessType.SAVE;
            } else if (StringUtils.equalsIgnoreCase(processType, ImportProcessType.REMOVE.getValue())) {
                importProcessType = ImportProcessType.REMOVE;
            } else if (StringUtils.equalsIgnoreCase(processType, ImportProcessType.FILE.getValue())) {
                importProcessType = ImportProcessType.FILE;
            } else {
                throw new NoSuchMethodException("İşlem türü Save,Remove veya File olmalıdır");
            }

            dataintegrationLogModel.setImportProcessType(importProcessType);
            if (BooleanUtils.isFalse(isSiteModel)
                    && CollectionUtils.isNotEmpty(allRows)) {
                var siteCode = allRows.get(0).get(DataIntegrationConstant.SITE_FIELD);
                if (StringUtils.isEmpty(siteCode)) {
                    throw new CsvValidationException("site(code)[unique] parametresi eksik");
                }
                var allSiteCodes = allRows.stream().map(a -> a.get(DataIntegrationConstant.SITE_FIELD)).collect(Collectors.toList());
                var allMatch = allSiteCodes.stream().allMatch(s -> s.equals(siteCode));
                siteModel = siteService.getSiteModel(siteCode);
                dataintegrationLogModel.setItemType(className);
                dataintegrationLogModel.setCount(allRows.size());
                dataintegrationLogModel.setStartDate(new Date());
                dataintegrationLogModel.setSite(siteModel);
                if (BooleanUtils.isFalse(allMatch)) {
                    dataintegrationLogModel.setStatus(DataIntegrationStatus.FAIL);

                    dataintegrationLogModel.setEndDate(new Date());

                    dataintegrationLogModel.setDescription("Site alanı tüm satırlar için aynı olmalıdır");
                    modelService.save(dataintegrationLogModel);
                    throw new CsvValidationException("Site alanı tüm satırlar için aynı olmalıdır");
                }

                dataintegrationLogModel.setStatus(DataIntegrationStatus.PROCESSING);
                modelService.save(dataintegrationLogModel);

                if (Objects.equals(ImportProcessType.FILE, importProcessType)) {
                    isSuccessPair = mediaImportService.importData(itemClass, true, allRows);
                } else {
                    siteModel = siteService.getSiteModel(allRows.get(0).get(DataIntegrationConstant.SITE_FIELD));
                    isSuccessPair = importService.importData(itemClass, allRows, importProcessType, siteModel);
                }

            } else if (BooleanUtils.isTrue(isSiteModel) && CollectionUtils.isNotEmpty(allRows)) {
                if (Objects.equals(ImportProcessType.FILE, importProcessType)) {
                    isSuccessPair = mediaImportService.importData(itemClass, true, allRows);
                } else {
                    try {
                        siteModel = siteService.getSiteModel(allRows.get(0).get(DataIntegrationConstant.CODE_FIELD));
                    } catch (Exception e) {
                        siteModel = null;
                    }
                    dataintegrationLogModel.setSite(siteModel);
                    isSuccessPair = importService.importData(itemClass, allRows, importProcessType, siteModel);
                }
            }

            if (isSuccessPair.getKey()) {
                dataintegrationLogModel.setStatus(DataIntegrationStatus.SUCCESS);
                dataintegrationLogModel.setEndDate(new Date());
                dataintegrationLogModel.setDescription(DataIntegrationStatus.SUCCESS.toString());
                modelService.save(dataintegrationLogModel);
                response.setStatus(ProcessStatus.SUCCESS);
                response.setDetail("Tüm veriler başarıyla içeri alındı");
                return response;
            } else {
                dataintegrationLogModel.setStatus(DataIntegrationStatus.FAIL);
                dataintegrationLogModel.setEndDate(new Date());
                dataintegrationLogModel.setDescription(DataIntegrationStatus.FAIL.toString());
                modelService.save(dataintegrationLogModel);
                throw new StoreRuntimeException("Given json body is invalid", "invalid.json.body.msg", new Object[]{isSuccessPair.getValue()});
            }

        } catch (final Exception e) {
            log.error("Exception occurred while importing datas...", e);
            if (BooleanUtils.isFalse(isSiteModel) && Objects.nonNull(siteModel)) {
                dataintegrationLogModel.setStatus(DataIntegrationStatus.FAIL);
                dataintegrationLogModel.setEndDate(new Date());
                var exceptionMessage = "";
                if (e instanceof StoreRuntimeException) {
                    exceptionMessage = messageSource.getMessage(((StoreRuntimeException) e).getMessageKey(), ((StoreRuntimeException) e).getArgs(), Messages.getMessagesLocale());
                } else {
                    exceptionMessage = ExceptionUtils.getMessage(e);
                }
                dataintegrationLogModel.setDescription(exceptionMessage);
                modelService.save(dataintegrationLogModel);
            }
            throw e;
        }


    }

    private void checkPermission(Class<? extends ItemModel> itemClass, String process) {
        var authorities = userService.getCurrentUserAuthorities();
        authorities.stream().filter(a -> StringUtils.equals(a, ServiceConstant.SUPER_ADMIN) ||
                        StringUtils.equals(a, itemClass.getSimpleName().concat(ServiceConstant.UNDERSCORE)
                                .concat(process))).findFirst()
                .orElseThrow(() -> new ModelReadException("You are not authorized to do process from [+" + StoreClassUtils.getSimpleName(itemClass) + "] table",
                        "search.service.not.authorized.read.table", new Object[]{StoreClassUtils.getSimpleName(itemClass)}));
    }
}
