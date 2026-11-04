package org.apache.hadoop.fs;

import java.util.Locale;

/**
 * Storage classes supported by the COS Java SDK.
 *
 * <p>The COS Java SDK no longer maintains the {@code StorageClass} enum for
 * newly introduced storage classes, so CosN accepts the storage class as a
 * plain string. This class centralizes the currently supported storage classes
 * and normalizes an arbitrary configured value to a canonical one. When the
 * configured value is not recognized, it falls back to {@link #STANDARD}.</p>
 */
public final class CosNStorageClass {

    public static final String MAZ_STANDARD = "MAZ_STANDARD";
    public static final String MAZ_STANDARD_IA = "MAZ_STANDARD_IA";
    public static final String MAZ_ARCHIVE = "MAZ_ARCHIVE";
    public static final String MAZ_DEEP_ARCHIVE = "MAZ_DEEP_ARCHIVE";
    public static final String MAZ_INTELLIGENT_TIERING = "MAZ_INTELLIGENT_TIERING";
    public static final String MAZ_COLD = "MAZ_COLD";
    public static final String STANDARD = "STANDARD";
    public static final String STANDARD_IA = "STANDARD_IA";
    public static final String ARCHIVE = "ARCHIVE";
    public static final String DEEP_ARCHIVE = "DEEP_ARCHIVE";
    public static final String INTELLIGENT_TIERING = "INTELLIGENT_TIERING";
    public static final String COLD = "COLD";
    public static final String EXPRESS_LITE = "EXPRESS_LITE";
    public static final String MAZ_EXPRESS_LITE = "MAZ_EXPRESS_LITE";

    private CosNStorageClass() {
    }

    /**
     * Resolves the configured storage class to a canonical value.
     *
     * @param storageClass the configured storage class, may be {@code null}
     * @return the canonical upper-case storage class when it is supported;
     *         {@code null} when the input is {@code null} or blank;
     *         {@link #STANDARD} when the input is not in the supported list
     */
    public static String resolve(String storageClass) {
        if (null == storageClass || storageClass.trim().isEmpty()) {
            return null;
        }

        String normalized = storageClass.trim().toUpperCase(Locale.ROOT);
        switch (normalized) {
            case MAZ_STANDARD:
            case MAZ_STANDARD_IA:
            case MAZ_ARCHIVE:
            case MAZ_DEEP_ARCHIVE:
            case MAZ_INTELLIGENT_TIERING:
            case MAZ_COLD:
            case STANDARD:
            case STANDARD_IA:
            case ARCHIVE:
            case DEEP_ARCHIVE:
            case INTELLIGENT_TIERING:
            case COLD:
            case EXPRESS_LITE:
            case MAZ_EXPRESS_LITE:
                return normalized;
            default:
                return STANDARD;
        }
    }
}
