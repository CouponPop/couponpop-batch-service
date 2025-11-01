package com.couponpop.batchservice.common.fcm.factory;

import com.google.firebase.messaging.*;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

@Component
public class FcmMessageFactory {

    private static final long ANDROID_TTL_MINUTES = 5;
    private static final String APNS_PRIORITY_HEADER = "apns-priority";
    private static final String APNS_PRIORITY_VALUE = "10";
    private static final String APNS_PUSH_TYPE_HEADER = "apns-push-type";
    private static final String APNS_PUSH_TYPE_VALUE = "alert";
    private static final String APS_SOUND_DEFAULT = "default";

    private static final String DATA_KEY_PLATFORM = "platform";
    private static final String PLATFORM_ANDROID = "android";
    private static final String PLATFORM_IOS = "ios";
    private static final String PLATFORM_WEB = "web";
    private static final String DATA_KEY_TITLE = "title";
    private static final String DATA_KEY_BODY = "body";

    public Message createMessage(String token, String title, String body) {
        Notification notification = createNotification(title, body);
        AndroidConfig androidConfig = createAndroidConfig();
        ApnsConfig apnsConfig = createApnsConfig();
        WebpushConfig webpushConfig = createWebpushConfig(title, body);

        return Message.builder()
                .setToken(token)
                .setNotification(notification)
                .setAndroidConfig(androidConfig)
                .setApnsConfig(apnsConfig)
                .setWebpushConfig(webpushConfig)
                .putData(DATA_KEY_TITLE, title)
                .putData(DATA_KEY_BODY, body)
                .build();
    }

    public MulticastMessage createMulticastMessage(List<String> tokens, String title, String body) {
        Notification notification = createNotification(title, body);
        AndroidConfig androidConfig = createAndroidConfig();
        ApnsConfig apnsConfig = createApnsConfig();
        WebpushConfig webpushConfig = createWebpushConfig(title, body);

        return MulticastMessage.builder()
                .addAllTokens(tokens)
                .setNotification(notification)
                .setAndroidConfig(androidConfig)
                .setApnsConfig(apnsConfig)
                .setWebpushConfig(webpushConfig)
                .putData(DATA_KEY_TITLE, title)
                .putData(DATA_KEY_BODY, body)
                .build();
    }

    private Notification createNotification(String title, String body) {
        return Notification.builder()
                .setTitle(title)
                .setBody(body)
                .build();
    }

    private WebpushNotification createWebpushNotification(String title, String body) {
        return WebpushNotification.builder()
                .setTitle(title)
                .setBody(body)
                .build();
    }

    private AndroidConfig createAndroidConfig() {
        return AndroidConfig.builder()
                .setPriority(AndroidConfig.Priority.HIGH)
                .setTtl(Duration.ofMinutes(ANDROID_TTL_MINUTES).toMillis())
                .putData(DATA_KEY_PLATFORM, PLATFORM_ANDROID)
                .build();
    }

    private ApnsConfig createApnsConfig() {
        return ApnsConfig.builder()
                .putHeader(APNS_PRIORITY_HEADER, APNS_PRIORITY_VALUE)
                .putHeader(APNS_PUSH_TYPE_HEADER, APNS_PUSH_TYPE_VALUE)
                .setAps(Aps.builder()
                        .setSound(APS_SOUND_DEFAULT)
                        .putCustomData(DATA_KEY_PLATFORM, PLATFORM_IOS)
                        .build())
                .build();
    }

    private WebpushConfig createWebpushConfig(String title, String body) {
        WebpushNotification webpushNotification = createWebpushNotification(title, body);

        return WebpushConfig.builder()
                .setNotification(webpushNotification)
                .putData(DATA_KEY_PLATFORM, PLATFORM_WEB)
                .build();
    }

}
