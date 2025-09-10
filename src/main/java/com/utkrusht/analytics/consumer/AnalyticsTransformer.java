package com.utkrusht.analytics.consumer;

public class AnalyticsTransformer {
    // Dummy stateless transformation that avoids memory bloat
    public static void transform(String analyticsJson) {
        // Example: parse, map fields, enrich, drop large payloads, etc.
        // Here, as a placeholder, just a fast check
        if (analyticsJson == null) return;
        // Could be replaced with object mapping/validation if needed
    }
}