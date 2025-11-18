package com.couponpop.batchservice.common.response;

import lombok.Getter;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@Getter
public class ApiResponse<T> {

    private final T data;

    private ApiResponse(T data) {
        this.data = data;
    }

    /**
     * 생성된 리소스에 대한 응답을 반환하는 메서드
     * 주어진 데이터를 포함하여 HTTP 201 Created 상태 코드와 함께 응답을 반환
     *
     * @param data 생성된 리소스의 데이터
     * @return HTTP 201 Created 응답과 함께 생성된 데이터가 포함된 ApiResponseDto
     */
    public static <T> ResponseEntity<ApiResponse<T>> created(T data) {
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(new ApiResponse<>(data));
    }

    /**
     * 성공적인 요청에 대한 응답을 반환하는 메서드
     * 주어진 데이터를 포함하여 HTTP 200 OK 상태 코드와 함께 응답을 반환
     *
     * @param data 요청 성공 시 반환할 데이터
     * @return HTTP 200 OK 응답과 함께 성공 데이터가 포함된 ApiResponseDto
     */
    public static <T> ResponseEntity<ApiResponse<T>> success(T data) {
        return ResponseEntity.ok(new ApiResponse<>(data));
    }

    /**
     * 성공적인 요청에 대한 응답을 반환하는 메서드
     * 주어진 데이터를 포함하여 HTTP 204 No Content 상태 코드와 함께 응답을 반환
     *
     * @return HTTP 204 No Content 응답과 함께 성공 데이터가 포함된 ApiResponseDto
     */
    public static <T> ResponseEntity<ApiResponse<T>> noContent() {
        return ResponseEntity.noContent().build();
    }
}
