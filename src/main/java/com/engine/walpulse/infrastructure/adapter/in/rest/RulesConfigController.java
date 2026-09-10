package com.engine.walpulse.infrastructure.adapter.in.rest;

import com.engine.walpulse.application.dto.TableFilterRule;
import com.engine.walpulse.application.dto.WalPulseProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Iterator;
import java.util.Map;

/**
 * REST controller for viewing and updating runtime filter & masking rules and testing in a sandbox.
 */
@RestController
@RequestMapping("/api/v1/config")
public class RulesConfigController {

    private final WalPulseProperties properties;
    private final ObjectMapper objectMapper;

    public RulesConfigController(WalPulseProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @GetMapping("/rules")
    public ResponseEntity<TableFilterRule> getRules() {
        return ResponseEntity.ok(properties.getFilter());
    }

    @PutMapping("/rules")
    public ResponseEntity<TableFilterRule> updateRules(@RequestBody TableFilterRule newRules) {
        if (newRules.getIncludeTables() != null) {
            properties.getFilter().setIncludeTables(newRules.getIncludeTables());
        }
        if (newRules.getExcludeTables() != null) {
            properties.getFilter().setExcludeTables(newRules.getExcludeTables());
        }
        if (newRules.getMaskedFields() != null) {
            properties.getFilter().setMaskedFields(newRules.getMaskedFields());
        }
        return ResponseEntity.ok(properties.getFilter());
    }

    @PostMapping("/sandbox")
    public ResponseEntity<Map<String, Object>> testSandbox(@RequestBody Map<String, Object> input) {
        String schema = input.containsKey("_schema") ? input.get("_schema").toString() : "public";
        String table = input.containsKey("_table") ? input.get("_table").toString() : "users";

        boolean allowed = properties.getFilter().isTableAllowed(schema, table);

        ObjectNode outputNode = objectMapper.createObjectNode();
        input.forEach((k, v) -> {
            if (properties.getFilter().isFieldMasked(k)) {
                outputNode.put(k, "[REDACTED]");
            } else {
                outputNode.set(k, objectMapper.valueToTree(v));
            }
        });

        return ResponseEntity.ok(Map.of(
                "schema", schema,
                "table", table,
                "allowed", allowed,
                "output", outputNode
        ));
    }
}
