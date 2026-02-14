package com.btc_store.dataintegration.service.impl;

import com.btc_store.dataintegration.enums.MergeStrategyEnum;
import com.btc_store.dataintegration.service.ImportService;
import com.btc_store.domain.enums.ImportProcessType;
import com.btc_store.domain.enums.SearchOperator;
import com.btc_store.domain.model.custom.SiteModel;
import com.btc_store.domain.model.custom.extend.ItemModel;
import com.btc_store.domain.model.custom.localize.Localized;
import com.btc_store.service.ModelService;
import com.btc_store.service.SearchService;
import com.btc_store.service.SiteService;
import com.btc_store.service.exception.StoreException;
import com.btc_store.service.exception.StoreRuntimeException;
import com.btc_store.service.exception.model.ModelNotFoundException;
import com.fasterxml.jackson.databind.ObjectMapper;
import constant.MessageConstant;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.context.MessageSource;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionSystemException;
import util.StoreClassUtils;
import util.Messages;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static com.btc_store.dataintegration.constant.DataIntegrationConstant.SITE_MODEL;

@Service
@RequiredArgsConstructor
@Slf4j
public class ImportServiceImpl implements ImportService {

    private static final String COLON = ":";
    private static final String SEMICOLON = ";";
    private static final String UNIQUE_POINTER = "[unique]";
    private static final String LANG_POINTER = "[lang=";
    private static final String CODE_POINTER = "(code)";
    private static final String[] datePatterns = new String[]{"dd.MM.yyyy", "ddMMyyyy", "yyyyMMdd", "dd.MM.yyyy HH:mm",
            "dd.MM.yyyy HH:mm:ss", "dd/MM/yyyy", "dd/MM/yyyy HH:mm", "dd/MM/yyyy HH:mm:ss", "yyyyMMddHHmmss", "yyyyMMdd HH:mm:ss"};


    protected final ModelService modelService;
    protected final SiteService siteService;
    protected final MessageSource messageSource;
    protected final SearchService searchService;
    protected final ObjectMapper objectMapper;


    @Override
    public Pair<Boolean, String> importData(Class itemType, List<Map<String, String>> data, ImportProcessType importProcessType, SiteModel siteModel) {
        Map<String, String> errorRow = null;
        var isAllDataValid = false;
        try {
            List<ItemModel> models = new ArrayList<>();

            for (Map<String, String> row : data) {
                //trim values
                for (var rowValue : row.entrySet()) {
                    row.put(rowValue.getKey(), StringUtils.trim(rowValue.getValue()));
                }
                errorRow = row;


                ItemModel model;
                var isNew = false;
                var uniqueFields = row.keySet().stream().filter(p -> StringUtils.contains(p, UNIQUE_POINTER)).collect(Collectors.toSet());
                var uniqueMapForQuery = new HashMap<>();
                var tempObj = modelService.create(itemType);
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

                        if (!StringUtils.equals(relationType, "site")) {
                            map.put("site", siteModel);
                        }
                        if (isNullRelation) {
                            uniqueMapForQuery.put(relationType, null);

                        } else {
                            Class tclass = PropertyUtils.getReadMethod(getPropertyDescriptor(tempObj, relationType)).getReturnType();
                            uniqueMapForQuery.put(StringUtils.remove(StringUtils.remove(uniqueField, UNIQUE_POINTER), CODE_POINTER), searchService.searchSingleResult(tclass, map, SearchOperator.AND));
                        }

                    } else {
                        var fieldName = clearFieldName(uniqueField);
                        uniqueMapForQuery.put(fieldName, convertWrapperType(getPropertyDescriptor(tempObj, StringUtils.remove(uniqueField, UNIQUE_POINTER)), null, MergeStrategyEnum.MERGE, row.get(uniqueField)));
                    }


                }

                try {
                    model = searchService.searchSingleResult(itemType, uniqueMapForQuery, SearchOperator.AND);
                } catch (ModelNotFoundException e) {
                    model = modelService.create(itemType);
                    isNew = true;
                } catch (Exception e) {
                    log.error("Error occurred while data finding on database...Error Row {}::Error Message {}", convertObjectToJson(errorRow), ExceptionUtils.getMessage(e));
                    throw new Exception("Error occurred while data was imported...Error Row  " + convertObjectToJson(errorRow) + " Error Message : " + ExceptionUtils.getMessage(e));
                }

                final Set<String> relations = row.keySet().stream().filter(p -> p.contains("(")).collect(Collectors.toSet());
                final Collection<String> notRelations = CollectionUtils.removeAll(row.keySet().stream().collect(Collectors.toSet()), relations);

                //relations...
                if (CollectionUtils.isNotEmpty(relations)) {
                    if (BooleanUtils.isFalse(isNew)) {
                        var relationsTypes = relations.stream().map(r -> StringUtils.split(r, "(")[0]).collect(Collectors.toSet());
                        model = searchService.searchSingleResultRelation(model, relationsTypes.stream().toArray(String[]::new));
                    }

                    for (String relation : relations) {
                        if (StringUtils.isEmpty(row.get(relation)) || StringUtils.equalsIgnoreCase(row.get(relation), "null")) {
                            continue;
                        }
                        String relationType = StringUtils.split(relation, "(")[0];
                        String[] contents = StringUtils.substringBetween(relation, "(", ")").split(COLON);

                        var relationContents = row.get(relation).split(SEMICOLON);
                        var relationMappingParams = new ArrayList<HashMap>();

                        for (String rel : relationContents) {
                            if (StringUtils.split(rel, COLON).length != contents.length) {
                                throw new IllegalArgumentException("Parameter count can not be matched.");
                            }

                            var map = new HashMap<>();
                            for (int i = 0; i < contents.length; i++) {
                                map.put(contents[i], StringUtils.split(rel, COLON)[i]);
                            }

                            relationMappingParams.add(map);
                        }

                        var propertyDescription = getPropertyDescriptor(model, relationType);
                        String typeName = PropertyUtils.getReadMethod(propertyDescription).getGenericReturnType().getTypeName();
                        //relation is set,collection,list...
                        if (StringUtils.startsWith(typeName, "java")) {


                            Collection<ItemModel> itemModels = findJavaType(typeName);
                            MergeStrategyEnum mergeStrategyEnum = MergeStrategyEnum.MERGE;
                            typeName = StringUtils.substringBetween(typeName, "<", ">");
                            if (StringUtils.contains(relation, "mode")) {
                                String mergeMode = StringUtils.remove(StringUtils.split(relation, "[")[1], "]").split("=")[1];
                                mergeStrategyEnum = MergeStrategyEnum.valueOf(StringUtils.toRootUpperCase(mergeMode));

                            }

                            if (MergeStrategyEnum.MERGE.equals(mergeStrategyEnum)) {

                                var relationItemModel = (Collection<ItemModel>) PropertyUtils.getSimpleProperty(model, relationType);
                                itemModels = CollectionUtils.isEmpty(relationItemModel) ? itemModels : relationItemModel;
                            }


                            Class rel = Class.forName(typeName);

                            for (Map param : relationMappingParams) {
                                if (!StringUtils.equals(StoreClassUtils.getSimpleName(rel), SITE_MODEL)
                                        && Objects.nonNull(siteModel)) {
                                    param.put("site", siteModel);
                                }
                                ItemModel itemModel = searchService.searchSingleResult(rel, param, SearchOperator.AND);
                                itemModels.add(itemModel);
                            }

                            PropertyUtils.setSimpleProperty(model, relationType, itemModels);


                        } else {
                            Class relationClass = Class.forName(typeName);

                            Map queryMap = new HashMap();
                            //content code,version
                            for (String ids : contents) {
                                if (!StringUtils.equals(StoreClassUtils.getSimpleName(relationClass), SITE_MODEL)
                                        && Objects.nonNull(siteModel)) {
                                    queryMap.put("site", siteModel);
                                }
                                queryMap.put(ids, row.get(relation));
                            }
                            PropertyUtils.setSimpleProperty(model, relationType, searchService.searchSingleResult(relationClass, queryMap, SearchOperator.AND));

                        }


                    }
                }

                if (CollectionUtils.isNotEmpty(notRelations)) {
                    for (String notRelation : notRelations) {

                        var fieldName = clearFieldName(notRelation);

                        if (StringUtils.contains(notRelation, LANG_POINTER)) {
                            var lang = StringUtils.substringBetween(notRelation, "[", "]").split("=")[1];
                            PropertyUtils.setSimpleProperty(model, fieldName, convertLangAttribute((Localized) PropertyUtils.getProperty(model, fieldName), Locale.forLanguageTag(lang), row.get(notRelation)));
                            continue;
                        }

                        var mergeMode = MergeStrategyEnum.MERGE;
                        if (StringUtils.contains(notRelation, "[mode=")) {
                            String mergeType = StringUtils.remove(StringUtils.split(notRelation, "[")[1], "]").split("=")[1];
                            mergeMode = MergeStrategyEnum.valueOf(StringUtils.toRootUpperCase(mergeType));

                        }

                        var propertyDescription = getPropertyDescriptor(model, fieldName);
                        PropertyUtils.setSimpleProperty(model, fieldName, convertWrapperType(propertyDescription, model, mergeMode, row.get(notRelation)));
                    }
                }
                models.add(model);
            }
            isAllDataValid = true;
            log.info("Starting import to database...");
            if (ImportProcessType.SAVE.equals(importProcessType)) {
                modelService.saveAll(models);
            } else {
                modelService.removeAll(models);
            }
            log.info("{} of {} datas have been imported successfully...", data.size(), itemType);
            return new ImmutablePair<>(Boolean.TRUE, String.format("{0} of {1} data has been imported successfully", data.size(), itemType));


        } catch (Throwable e) {
            if (isAllDataValid) {
                log.error("Error occurred while data was imported..." + exceptionMessage(e));
                return new ImmutablePair<>(Boolean.FALSE, "Error occurred while data was imported..." + exceptionMessage(e));
            } else {
                log.error("Error occurred ..Error Row {}::Error Message {}", convertObjectToJson(errorRow), exceptionMessage(e));
                return new ImmutablePair<>(Boolean.FALSE, "Error occurred while data was imported...Error Row : " + convertObjectToJson(errorRow) + " Error Message : " + exceptionMessage(e));
            }

        }

    }

    private String exceptionMessage(Throwable ex) {

        var message = StringUtils.EMPTY;
        if (ex instanceof StoreRuntimeException && StringUtils.isNotEmpty(((StoreRuntimeException) ex).getMessageKey())) {
            message = messageSource.getMessage(((StoreRuntimeException) ex).getMessageKey(), ((StoreRuntimeException) ex).getArgs(), Messages.getMessagesLocale());

        } else if (ex instanceof StoreException && StringUtils.isNotEmpty(((StoreException) ex).getMessageKey())) {
            message = messageSource.getMessage(((StoreException) ex).getMessageKey(), ((StoreException) ex).getArgs(), Messages.getMessagesLocale());
        } else if (ex instanceof TransactionSystemException && ExceptionUtils.getRootCause(ex) instanceof ConstraintViolationException) {
            ConstraintViolationException modelValidatorEx =
                    ((ConstraintViolationException) ((TransactionSystemException) ex).getRootCause());
            Set<ConstraintViolation<?>> constraintViolations = modelValidatorEx.getConstraintViolations();
            if (CollectionUtils.isNotEmpty(constraintViolations)) {
                message = messageSource.getMessage(StringUtils.substringBetween(constraintViolations.iterator().next().getMessage(), "{", "}"), null, Messages.getMessagesLocale());
            } else {
                message = ExceptionUtils.getMessage(ex);
            }
        } else if (ex instanceof DataIntegrityViolationException) {
            message = messageSource.getMessage(MessageConstant.DATA_INTEGRATION_VIOLATION_EXCEPTION, null, Messages.getMessagesLocale());

        } else if (ex instanceof DataAccessResourceFailureException) {
            message = messageSource.getMessage(MessageConstant.DATA_ACCESS_RESOURCE_EXCEPTION, null, Messages.getMessagesLocale());
        } else {
            message = ExceptionUtils.getMessage(ex);
        }

        return message;
    }

    private PropertyDescriptor getPropertyDescriptor(ItemModel model, String fieldName) throws
            NoSuchFieldException,
            IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        var propertyDescription = PropertyUtils.getPropertyDescriptor(model, fieldName);
        if (Objects.isNull(propertyDescription)) {
            log.error("This field {} not contains on model {}...", fieldName, model);
            throw new NoSuchFieldException(String.format("%s alanı %s tablosunda yer alan bir alan değildir.",
                    fieldName, StoreClassUtils.getSimpleName(model)));
        }
        return propertyDescription;
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

    private Localized convertLangAttribute(Localized localized, Locale locale, String value) {
        if (Objects.isNull(localized)) {
            localized = new Localized();
            localized.setValue(locale, value);
            return localized;
        } else {
            localized.setValue(locale, value);
            return localized;
        }
    }

    private String clearFieldName(String fieldName) {
        var content = StringUtils.substringBetween(fieldName, "[", "]");
        fieldName = StringUtils.remove(fieldName, content);
        fieldName = StringUtils.remove(fieldName, "[");
        fieldName = StringUtils.remove(fieldName, "]");
        return fieldName;
    }

    private Collection<ItemModel> findJavaType(String typeName) {
        if (StringUtils.startsWith(typeName, "java.util.Set")) {
            return new HashSet<>();
        } else {
            return new ArrayList<>();
        }
    }

    @SneakyThrows
    private Object convertWrapperType(PropertyDescriptor propertyDescriptor, ItemModel model,
                                      MergeStrategyEnum mergeStrategyEnum,
                                      String value) throws ParseException {

        Class tClass = PropertyUtils.getReadMethod(propertyDescriptor).getReturnType();

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
                try {
                    return Double.valueOf(value);
                } catch (NumberFormatException e) {
                    return NumberUtils.DOUBLE_ZERO;
                }
            case "Integer":
            case "int":
                try {
                    return Integer.valueOf(value);
                } catch (NumberFormatException e) {
                    return NumberUtils.INTEGER_ZERO;
                }
            case "Float":
            case "float":
                try {
                    return Float.valueOf(value);
                } catch (NumberFormatException e) {
                    return NumberUtils.FLOAT_ZERO;
                }
            case "Long":
            case "long":
                try {
                    return Long.valueOf(value);
                } catch (NumberFormatException e) {
                    return NumberUtils.LONG_ZERO;
                }
            case "boolean":
            case "Boolean":
                return BooleanUtils.toBoolean(value);

            case "BigDecimal":
                try {
                    return new BigDecimal(value);
                } catch (NumberFormatException e) {
                    return BigDecimal.ZERO;
                }

            case "Date":
                try {
                    return DateUtils.parseDateStrictly(value, datePatterns);
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
                        if (propertyDescriptor.getPropertyType().equals(List.class)) {
                            if (Objects.equals(mergeStrategyEnum, MergeStrategyEnum.MERGE)) {
                                var records = Optional.ofNullable((Collection<Integer>) PropertyUtils.getSimpleProperty(model, propertyDescriptor.getName())).orElse(new ArrayList<>());
                                records.addAll(new ArrayList<>(splitedValuesStream.map(Integer::valueOf).collect(Collectors.toSet())));
                                return records;
                            } else {
                                return new ArrayList<>(splitedValuesStream.map(Integer::valueOf).collect(Collectors.toSet()));
                            }
                        } else {
                            if (Objects.equals(mergeStrategyEnum, MergeStrategyEnum.MERGE)) {
                                var records = Optional.ofNullable((Collection<Integer>) PropertyUtils.getSimpleProperty(model, propertyDescriptor.getName())).orElse(new HashSet<>());
                                records.addAll(new HashSet<>(splitedValuesStream.map(Integer::valueOf).collect(Collectors.toSet())));
                                return records;
                            } else {
                                return new HashSet<>(splitedValuesStream.map(Integer::valueOf).collect(Collectors.toSet()));
                            }
                        }

                    case "Double":
                        if (propertyDescriptor.getPropertyType().equals(List.class)) {
                            if (Objects.equals(mergeStrategyEnum, MergeStrategyEnum.MERGE)) {
                                var records = Optional.ofNullable((Collection<Double>) PropertyUtils.getSimpleProperty(model, propertyDescriptor.getName())).orElse(new ArrayList<>());
                                records.addAll(new ArrayList<>(splitedValuesStream.map(Double::valueOf).collect(Collectors.toSet())));
                                return records;
                            } else {
                                return new ArrayList<>(splitedValuesStream.map(Double::valueOf).collect(Collectors.toSet()));
                            }
                        } else {
                            if (Objects.equals(mergeStrategyEnum, MergeStrategyEnum.MERGE)) {
                                var records = Optional.ofNullable((Collection<Double>) PropertyUtils.getSimpleProperty(model, propertyDescriptor.getName())).orElse(new HashSet<>());
                                records.addAll(new HashSet<>(splitedValuesStream.map(Double::valueOf).collect(Collectors.toSet())));
                                return records;
                            } else {
                                return new HashSet<>(splitedValuesStream.map(Double::valueOf).collect(Collectors.toSet()));
                            }
                        }
                    default:
                        if (propertyDescriptor.getPropertyType().equals(List.class)) {

                            if (Objects.equals(mergeStrategyEnum, MergeStrategyEnum.MERGE)) {
                                var records = Optional.ofNullable((Collection<String>) PropertyUtils.getSimpleProperty(model, propertyDescriptor.getName())).orElse(new ArrayList<>());
                                records.addAll(new ArrayList<>(splitedValuesStream.map(String::valueOf).collect(Collectors.toSet())));
                                return records;
                            } else {
                                return new ArrayList<>(splitedValuesStream.map(String::valueOf).collect(Collectors.toSet()));
                            }

                        } else {
                            if (Objects.equals(mergeStrategyEnum, MergeStrategyEnum.MERGE)) {
                                var records = Optional.ofNullable((Collection<String>) PropertyUtils.getSimpleProperty(model, propertyDescriptor.getName())).orElse(new HashSet<>());
                                records.addAll(new HashSet<>(splitedValuesStream.map(String::valueOf).collect(Collectors.toSet())));
                                return records;
                            } else {
                                return new HashSet<>(splitedValuesStream.map(String::valueOf).collect(Collectors.toSet()));
                            }
                        }
                }
            case "Duration":
                return Duration.parse(value);

            default:
                return value;

        }
    }

}
