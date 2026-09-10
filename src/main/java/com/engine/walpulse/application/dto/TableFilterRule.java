package com.engine.walpulse.application.dto;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Filter configuration for table inclusion/exclusion and sensitive field masking.
 */
public class TableFilterRule {

    private List<String> includeTables = List.of(".*");
    private List<String> excludeTables = List.of();
    private List<String> maskedFields = List.of("password", "token", "secret", "credit_card", "ssn");

    private List<Pattern> includePatterns = List.of(Pattern.compile(".*"));
    private List<Pattern> excludePatterns = List.of();

    public boolean isTableAllowed(String schema, String table) {
        String full = (schema != null && !schema.isBlank() ? schema + "." : "") + table;

        boolean matchesInclude = includePatterns.stream().anyMatch(p -> p.matcher(full).matches() || p.matcher(table).matches());
        if (!matchesInclude) {
            return false;
        }

        boolean matchesExclude = excludePatterns.stream().anyMatch(p -> p.matcher(full).matches() || p.matcher(table).matches());
        return !matchesExclude;
    }

    public boolean isFieldMasked(String fieldName) {
        if (fieldName == null) return false;
        String lower = fieldName.toLowerCase();
        return maskedFields.stream().anyMatch(m -> lower.contains(m.toLowerCase()));
    }

    public List<String> getIncludeTables() {
        return includeTables;
    }

    public void setIncludeTables(List<String> includeTables) {
        this.includeTables = includeTables;
        this.includePatterns = includeTables.stream()
                .map(TableFilterRule::globToRegex)
                .map(Pattern::compile)
                .toList();
    }

    public List<String> getExcludeTables() {
        return excludeTables;
    }

    public void setExcludeTables(List<String> excludeTables) {
        this.excludeTables = excludeTables;
        this.excludePatterns = excludeTables.stream()
                .map(TableFilterRule::globToRegex)
                .map(Pattern::compile)
                .toList();
    }

    public List<String> getMaskedFields() {
        return maskedFields;
    }

    public void setMaskedFields(List<String> maskedFields) {
        this.maskedFields = maskedFields;
    }

    private static String globToRegex(String glob) {
        String trimmed = glob.trim();
        if (trimmed.contains("*") || trimmed.contains("?")) {
            return "^" + trimmed
                    .replace(".", "\\.")
                    .replace("*", ".*")
                    .replace("?", ".") + "$";
        }
        return "^" + Pattern.quote(trimmed) + "$";
    }
}
