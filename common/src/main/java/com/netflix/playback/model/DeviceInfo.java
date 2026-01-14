package com.netflix.playback.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Information about the device emitting playback events.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeviceInfo {

    /** Type of device (iOS, Android, etc.) */
    private DeviceType deviceType;

    /** Device model (e.g., "iPhone 15 Pro", "Samsung Smart TV") */
    private String deviceModel;

    /** Operating system version */
    private String osVersion;

    /** Application version */
    private String appVersion;

    /** IP address of the device */
    private String ipAddress;

    /** Country code (ISO 3166-1 alpha-2) */
    private String country;

    /** Region/state code */
    private String region;

    /** City name */
    private String city;
}
