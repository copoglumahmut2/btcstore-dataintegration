package com.btc_store.dataintegration.helper;

import com.btc_store.domain.data.custom.restservice.ServiceResponseData;
import com.btc_store.domain.enums.ProcessStatus;
import com.btc_store.service.exception.StoreException;
import com.btc_store.service.exception.StoreRuntimeException;
import constant.MessageConstant;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.context.MessageSource;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import util.Messages;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

@ControllerAdvice
@AllArgsConstructor
@Slf4j
public class ExceptionHelper {

    private final MessageSource messageSource;

    @ExceptionHandler(value = {StoreRuntimeException.class})
    public ResponseEntity<ServiceResponseData> handleBambooRuntimeException(StoreRuntimeException ex) {
        log.error(ExceptionUtils.getMessage(ex));
        return new ResponseEntity<>(fillServiceResponseData(ex), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @ExceptionHandler(value = {IllegalArgumentException.class})
    public ResponseEntity<ServiceResponseData> handleIllegalArgumentException(IllegalArgumentException ex) {
        log.error(String.join("Illegal Argument Exception: ", ExceptionUtils.getMessage(ex)));
        return new ResponseEntity<>(fillServiceResponseData(ex), HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(value = {TransactionSystemException.class})
    public ResponseEntity<ServiceResponseData> handleTransactionSystemException(TransactionSystemException ex) {
        log.error(String.join("TransactionSystem Exception: ", ExceptionUtils.getMessage(ex)));
        return new ResponseEntity<>(fillServiceResponseData(ex), HttpStatus.INTERNAL_SERVER_ERROR);
    }


    @ExceptionHandler(value = {Exception.class})
    public ResponseEntity<ServiceResponseData> handleException(Exception ex) {
        log.error(String.join("Exception: ", ExceptionUtils.getMessage(ex)));
        return new ResponseEntity<>(fillServiceResponseData(ex), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    private ServiceResponseData fillServiceResponseData(Exception ex) {
        var serviceResponseData = new ServiceResponseData();
        serviceResponseData.setStatus(ProcessStatus.ERROR);

        if (ex instanceof StoreRuntimeException && StringUtils.isNotEmpty(((StoreRuntimeException) ex).getMessageKey())) {
            serviceResponseData.setErrorMessage(messageSource.getMessage(((StoreRuntimeException) ex).getMessageKey(), ((StoreRuntimeException) ex).getArgs(), Messages.getMessagesLocale()));

        } else if (ex instanceof StoreException && StringUtils.isNotEmpty(((StoreException) ex).getMessageKey())) {
            serviceResponseData.setErrorMessage(messageSource.getMessage(((StoreException) ex).getMessageKey(), ((StoreException) ex).getArgs(), Messages.getMessagesLocale()));
        } else if (ex instanceof TransactionSystemException && ExceptionUtils.getRootCause(ex) instanceof ConstraintViolationException) {
            ConstraintViolationException modelValidatorEx =
                    ((ConstraintViolationException) ((TransactionSystemException) ex).getRootCause());
            Set<ConstraintViolation<?>> constraintViolations = modelValidatorEx.getConstraintViolations();
            if (CollectionUtils.isNotEmpty(constraintViolations)){
                var localeMessage = messageSource.getMessage(StringUtils.substringBetween(constraintViolations.iterator().next().getMessage(), "{", "}"), null, Messages.getMessagesLocale());
                serviceResponseData.setErrorMessage(localeMessage);
            }else {
                serviceResponseData.setErrorMessage(ExceptionUtils.getMessage(ex));
            }
        } else if (ex instanceof DataIntegrityViolationException) {
            serviceResponseData.setErrorMessage(messageSource.getMessage(MessageConstant.DATA_INTEGRATION_VIOLATION_EXCEPTION, null, Messages.getMessagesLocale()));

        }else if (ex instanceof DataAccessResourceFailureException){
            serviceResponseData.setErrorMessage(messageSource.getMessage(MessageConstant.DATA_ACCESS_RESOURCE_EXCEPTION, null, Messages.getMessagesLocale()));
        }
        else {
            serviceResponseData.setErrorMessage(ExceptionUtils.getMessage(ex));
        }
        serviceResponseData.setErrorMessageDetail(Arrays.asList(ExceptionUtils.getStackFrames(ex)).stream().limit(50).collect(Collectors.joining()));

        return serviceResponseData;
    }


}
