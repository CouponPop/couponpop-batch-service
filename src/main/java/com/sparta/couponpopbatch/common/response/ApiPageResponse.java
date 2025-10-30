package com.sparta.couponpopbatch.common.response;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;

import java.io.Serializable;
import java.util.List;

@Getter
@NoArgsConstructor
public class ApiPageResponse<T> implements Serializable {

    private int page;
    private int size;
    private int totalPages;
    private long totalElements;
    private List<T> data;

    private ApiPageResponse(int page, int size, int totalPages, long totalElements, List<T> data) {
        this.page = page;
        this.size = size;
        this.totalPages = totalPages;
        this.totalElements = totalElements;
        this.data = data;
    }

    /**
     * 성공적인 요청에 대한 페이징 응답을 반환하는 메서드 주어진 데이터를 포함하여 HTTP 200 OK 상태 코드와 함께 응답을 반환
     *
     * @param pagedData 요청 성공 시 반환할 페이징 데이터
     * @return HTTP 200 OK 응답과 함께 성공 데이터가 포함된 ApiPageResponse
     */
    public static <T> ResponseEntity<ApiPageResponse<T>> success(Page<T> pagedData) {
        return ResponseEntity.ok(fromPage(pagedData));
    }

    private static <T> ApiPageResponse<T> fromPage(Page<T> pagedData) {
        return new ApiPageResponse<>(
                pagedData.getPageable().getPageNumber(),
                pagedData.getPageable().getPageSize(),
                pagedData.getTotalPages(),
                pagedData.getTotalElements(),
                pagedData.getContent()
        );
    }
}