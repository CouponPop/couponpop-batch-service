package com.couponpop.batchservice.common.client;

import com.couponpop.batchservice.common.response.ApiResponse;
import com.couponpop.couponpopcoremodule.dto.store.response.StoreIdsByDongResponse;
import com.couponpop.couponpopcoremodule.dto.store.response.StoreRegionInfoResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.List;

@FeignClient(name = "${client.store-service.name}", url = "${client.store-service.url}")
public interface StoreFeignClient {

    @PostMapping("/internal/batch/v1/stores/regions")
    ApiResponse<List<StoreRegionInfoResponse>> fetchStoresRegionByIds(@RequestBody List<Long> storeIds);

    @PostMapping("/internal/batch/v1/stores/search")
    ApiResponse<List<StoreIdsByDongResponse>> fetchStoreIdsByDongs(@RequestBody List<String> dongs);
}
