package com.couponpop.batchservice.common.exception;

import com.couponpop.batchservice.common.response.ApiErrorResponse;
import com.google.firebase.FirebaseException;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.support.DefaultMessageSourceResolvable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import static java.util.stream.Collectors.joining;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(GlobalException.class)
    public ResponseEntity<ApiErrorResponse> handleGlobalException(GlobalException ex, HttpServletRequest request) {
        log.error("비즈니스 오류 발생 ", ex);
        return handleExceptionInternal(ex.getErrorCode(), request);
    }

    // @Valid 실패 시 발생하는 예외를 처리, message가 여러 개일 수 있으므로 묶어서 반환
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiErrorResponse> handleValidationExceptions(MethodArgumentNotValidException ex, HttpServletRequest request) {

        String errorsMessages = ex.getBindingResult().getFieldErrors().stream()
                .map(DefaultMessageSourceResolvable::getDefaultMessage)
                .collect(joining("\n"));

        return handleExceptionInternal(HttpStatus.BAD_REQUEST, errorsMessages, request);
    }

    @ExceptionHandler(FirebaseException.class)
    public ResponseEntity<ApiErrorResponse> handleFirebaseException(FirebaseException ex, HttpServletRequest request) {
        log.error("Firebase 오류 발생 ", ex);
        return handleExceptionInternal(HttpStatus.INTERNAL_SERVER_ERROR, "Firebase 서비스 오류가 발생했습니다.", request);
    }

    private ResponseEntity<ApiErrorResponse> handleExceptionInternal(ErrorCode errorCode, HttpServletRequest request) {
        return ResponseEntity
                .status(errorCode.getHttpStatus())
                .body(ApiErrorResponse.from(errorCode, request));
    }

    private ResponseEntity<ApiErrorResponse> handleExceptionInternal(HttpStatus httpStatus, String message, HttpServletRequest request) {
        return ResponseEntity
                .status(httpStatus)
                .body(ApiErrorResponse.from(httpStatus, message, request));
    }
}
