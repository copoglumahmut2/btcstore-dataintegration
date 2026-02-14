package com.btc_store.dataintegration.service.impl;

import com.btc_store.dataintegration.enums.MergeStrategyEnum;
import com.btc_store.dataintegration.service.MediaImportService;
import com.btc_store.domain.enums.MediaCategory;
import com.btc_store.domain.enums.SearchOperator;
import com.btc_store.domain.model.custom.MediaModel;
import com.btc_store.domain.model.custom.SiteModel;
import com.btc_store.domain.model.custom.extend.ItemModel;
import com.btc_store.service.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import util.StoreClassUtils;

import java.beans.PropertyDescriptor;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class MediaImportServiceImpl implements MediaImportService {

    private static final String COLON = ":";
    private static final String SEMICOLON = ";";
    private static final String UNIQUE_POINTER = "[unique]";
    private static final String CODE_POINTER = "(code)";
    private static final String SITE_FIELD = "site(code)[unique]";
    private static final String PATH = "path";
    private static final String URL = "url";
    private static final String MEDIACATEGORY = "mediaCategory";
    private static final String SECURE = "secure";
    private static final String GALLERIES = "galleries";
    private static final String SITE_MODEL = "SiteModel";

    @Value("${csv.file.upload.folder.path}")
    private String defaultUploadPath;


    protected final ModelService modelService;
    protected final SiteService siteService;
    protected final SearchService searchService;
    protected final ObjectMapper objectMapper;
    protected final MediaService mediaService;
    protected final CmsCategoryService cmsCategoryService;

    @Override
    public Pair<Boolean, String> importData(Class itemType, boolean move, List<Map<String, String>> data) {
        var errorRows = new ArrayList<Map<String, String>>();
        SiteModel siteModel = null;
        for (Map<String, String> row : data) {
            try {

                //site field is unique all tables but not for itself :)
                if (!StringUtils.equals(StoreClassUtils.getSimpleName(itemType), SITE_MODEL)) {
                    siteModel = siteService.getSiteModel(row.get(SITE_FIELD));
                }
                var mediaCategory = getRowField(row, MEDIACATEGORY);
                mediaCategory = StringUtils.isEmpty(mediaCategory) ? MediaCategory.OTHER.getValue() : mediaCategory;

                ItemModel model;
                var uniqueFields = row.keySet().stream().filter(p -> StringUtils.contains(p, UNIQUE_POINTER)).collect(Collectors.toSet());
                var tempObj = modelService.create(itemType);
                var uniqueMapForQuery = new HashMap<>();
                for (var uniqueField : uniqueFields) {

                    // if relation field is unique...
                    if (StringUtils.contains(uniqueField, "(")) {
                        var relationType = StringUtils.split(uniqueField, "(")[0];
                        var contents = StringUtils.substringBetween(uniqueField, "(", ")").split(COLON);

                        var relationContents = row.get(uniqueField).split(SEMICOLON);
                        var map = new HashMap<>();
                        for (String rel : relationContents) {
                            if (StringUtils.split(rel, COLON).length != contents.length) {
                                throw new IllegalArgumentException("Parameter count can not be matched.");
                            }


                            for (int i = 0; i < contents.length; i++) {
                                map.put(contents[i], StringUtils.split(rel, COLON)[i]);
                            }

                        }

                        var isNullRelation = map.keySet().stream().anyMatch(p -> StringUtils.isEmpty((String) map.get(p)) || StringUtils.equalsIgnoreCase((String) map.get(p), "null"));

                        if (isNullRelation) {
                            uniqueMapForQuery.put(relationType, null);

                        } else {
                            Class tclass = PropertyUtils.getReadMethod(PropertyUtils.getPropertyDescriptor(tempObj, relationType)).getReturnType();
                            uniqueMapForQuery.put(StringUtils.remove(StringUtils.remove(uniqueField, UNIQUE_POINTER), CODE_POINTER), searchService.searchSingleResult(tclass, map, SearchOperator.AND));
                        }

                    } else {
                        var fieldName = clearFieldName(uniqueField);
                        uniqueMapForQuery.put(fieldName, convertWrapperType(PropertyUtils.getPropertyDescriptor(tempObj, StringUtils.remove(uniqueField, UNIQUE_POINTER)), row.get(uniqueField)));
                    }

                }

                //find model given unique fields...
                model = searchService.searchSingleResult(itemType, uniqueMapForQuery, SearchOperator.AND);
                var fieldCellValue = getCellFieldValue(row);
                PropertyDescriptor propertyDescription = getPropertyDescriptor(model, fieldCellValue);
                String typeName = PropertyUtils.getReadMethod(propertyDescription).getGenericReturnType().getTypeName();

                //Dosya okunacak yer disk veya url olabilir.Öncelikle Path alanı var ise onu baz almalıyız.Yoksa url alanından okuyacağız.

                var isPath = StringUtils.isNotEmpty(getRowField(row, PATH));
                var isUrl = StringUtils.isNotEmpty(getRowField(row, URL));

                File tempFile = null;
                if (isUrl) {
                    tempFile = new File(StringUtils.join(defaultUploadPath, "/temp/",
                            StringUtils.substringAfterLast(new URL(getRowField(row, URL)).getPath(), "/")));
                }


                var path = Path.of(StringUtils.join(defaultUploadPath, getRowField(row, PATH))).normalize();
                var url = getRowField(row, URL);
                if (StringUtils.startsWith(typeName, "java")) {
                    model = searchService.searchSingleResultRelation(model, fieldCellValue);
                    Collection<ItemModel> itemModels = findJavaType(typeName);
                    var relationItemModel = (Collection<ItemModel>) PropertyUtils.getProperty(model, fieldCellValue);

                    MergeStrategyEnum mergeStrategyEnum = MergeStrategyEnum.MERGE;
                    var rowFieldHeader = row.entrySet().stream().filter(e -> e.getKey().contains("field")).findFirst().get().getKey();
                    if (StringUtils.contains(rowFieldHeader, "mode")) {
                        String mergeMode = StringUtils.remove(StringUtils.split(rowFieldHeader, "[")[1], "]").split("=")[1];
                        mergeStrategyEnum = MergeStrategyEnum.valueOf(StringUtils.toRootUpperCase(mergeMode));

                    }


                    Collection<MediaModel> medias = null;
                    if (MergeStrategyEnum.MERGE.equals(mergeStrategyEnum)) {
                        itemModels = CollectionUtils.isEmpty(relationItemModel) ? itemModels : relationItemModel;
                    } else {

                            medias = (Collection<MediaModel>) PropertyUtils.getProperty(model, fieldCellValue);
                            PropertyUtils.setSimpleProperty(model, fieldCellValue, itemModels);



                    }

                    File file;
                    if (isPath) {
                        file = path.toFile();
                    } else {
                        FileUtils
                                .copyURLToFile(new URL(url), tempFile);
                        file = tempFile;
                    }

                    if (file.isFile()) {
                        ItemModel mediaModel;

                            mediaModel = mediaService.storage(file, BooleanUtils.toBoolean(getRowField(row, SECURE)),
                                    !isUrl && move, cmsCategoryService.getCmsCategoryByCode(mediaCategory, siteModel), siteModel);



                        itemModels.add(mediaModel);
                        PropertyUtils.setSimpleProperty(model, fieldCellValue, itemModels);
                        modelService.save(model);
                        log.info("Imported file..." + convertObjectToJson(row));
                        if (Objects.equals(MergeStrategyEnum.OVERRIDE, mergeStrategyEnum)) {
                            if (CollectionUtils.isNotEmpty(medias)) {
                                medias.forEach(m -> m.setDeleted(true));
                                modelService.saveAll(mediaModel);
                            }

                        }
                    } else {
                        errorRows.add(row);
                    }


                } else {

                    File file;
                    if (isPath) {
                        file = path.toFile();
                    } else {
                        FileUtils
                                .copyURLToFile(new URL(url), tempFile);
                        file = tempFile;
                    }

                    if (file.isFile()) {
                        var mediaModel = mediaService.storage(file, BooleanUtils.toBoolean(getRowField(row, SECURE)),
                                isUrl ? false : move, cmsCategoryService.getCmsCategoryByCode(mediaCategory, siteModel), siteModel);

                        PropertyUtils.setSimpleProperty(model, fieldCellValue, mediaModel);
                        modelService.save(model);
                        log.info("Imported file..." + convertObjectToJson(row));
                    } else {
                        errorRows.add(row);
                    }

                }
                if (Objects.nonNull(tempFile) && tempFile.isFile()) {
                    FileUtils.delete(tempFile);
                }

            } catch (Throwable e) {
                log.error("Row exception : " + ExceptionUtils.getMessage(e));
                errorRows.add(row);
            }
        }
        if (CollectionUtils.isNotEmpty(errorRows)) {
            log.error("Some items could not be imported");
            return new ImmutablePair<>(Boolean.FALSE, "ERROR ROWS: " + convertObjectToJson(errorRows));
        } else {
            log.info(String.format("%s of %s finished file integration...", data.size(), itemType));
            return new ImmutablePair<>(Boolean.TRUE, String.format("%s of %s finished file integration...", data.size(), itemType));
        }
    }

    private String getCellFieldValue(Map<String, String> row) {
        if (StringUtils.isNotEmpty(row.get("field"))) {
            return row.get("field");
        } else if (StringUtils.isNotEmpty(row.get("field[mode=override]"))) {
            return row.get("field[mode=override]");
        } else {
            return row.get("field[mode=merge]");
        }
    }

    private String convertObjectToJson(Object value) {
        try {
            if (Objects.isNull(value)) {
                return StringUtils.EMPTY;
            }
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            return StringUtils.EMPTY;
        }
    }

    private String getRowField(Map<String, String> row, String fieldName) {
        return row.get(fieldName);
    }

    private Collection<ItemModel> findJavaType(String typeName) {
        if (StringUtils.startsWith(typeName, "java.util.Set")) {
            return new HashSet<>();
        } else {
            return new ArrayList<>();
        }
    }

    private PropertyDescriptor getPropertyDescriptor(ItemModel model, String fieldName) throws
            NoSuchFieldException,
            IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        var propertyDescription = PropertyUtils.getPropertyDescriptor(model, fieldName);
        if (Objects.isNull(propertyDescription)) {
            log.error("This field {} not contains on model {}...", fieldName, model);
            throw new NoSuchFieldException(String.format("This field %s not contains on model %s...", fieldName, model));
        }
        return propertyDescription;
    }

    private String clearFieldName(String fieldName) {
        var content = StringUtils.substringBetween(fieldName, "[", "]");
        fieldName = StringUtils.remove(fieldName, content);
        fieldName = StringUtils.remove(fieldName, "[");
        fieldName = StringUtils.remove(fieldName, "]");
        return fieldName;
    }

    private Object convertWrapperType(PropertyDescriptor propertyDescriptor, String value) throws ParseException {

        Class tClass = PropertyUtils.getReadMethod(propertyDescriptor).getReturnType();

        //boolean empty default false...
        if (StringUtils.isEmpty(value)
                && !StringUtils.equalsIgnoreCase(tClass.getSimpleName(), "boolean")) {
            return null;
        }

        if (BooleanUtils.isTrue(tClass.isEnum())) {
            var enumConstant = tClass.getEnumConstants();
            var enumValue = Arrays.asList(enumConstant)
                    .stream().filter(e -> StringUtils.equalsIgnoreCase(String.valueOf(e), value))
                    .findFirst().orElseThrow(NoSuchFieldError::new);
            return enumValue;
        }

        switch (tClass.getSimpleName()) {
            case "Double":
            case "double":
                return Double.valueOf(value);
            case "Integer":
            case "int":
                return Integer.valueOf(value);
            case "Float":
            case "float":
                return Float.valueOf(value);
            case "Long":
            case "long":
                return Long.valueOf(value);
            case "boolean":
            case "Boolean":
                return BooleanUtils.toBoolean(value);

            case "BigDecimal":
                return new BigDecimal(value);

            case "Date":
                try {
                    return DateUtils.parseDateStrictly(value, "dd.MM.yyyy", "dd.MM.yyyy HH:mm", "dd.MM.yyyy HH:mm:ss", "dd/MM/yyyy", "dd/MM/yyyy HH:mm", "dd/MM/yyyy HH:mm:ss");
                } catch (final ParseException e) {
                    log.error("Given date {} is not valid.", value);
                    throw new ParseException("Given date " + value + " is not valid", -1);
                }

            case "Set":
            case "List":
                var rawType = StringUtils.substringBetween(propertyDescriptor.getReadMethod().getGenericReturnType().getTypeName(), "<", ">");
                var splitedValuesStream = Arrays.asList(StringUtils.split(value, SEMICOLON)).stream();
                switch (StringUtils.remove(rawType, "java.lang.")) {
                    case "Integer":
                        return new HashSet<>(splitedValuesStream.map(v -> Integer.valueOf(v)).collect(Collectors.toSet()));
                    case "Double":
                        return new HashSet<>(splitedValuesStream.map(v -> Double.valueOf(v)).collect(Collectors.toSet()));
                    default:
                        return new HashSet<>(splitedValuesStream.map(v -> String.valueOf(v)).collect(Collectors.toSet()));
                }

            default:
                return value;

        }

    }

}

