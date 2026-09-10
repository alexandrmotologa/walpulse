package com.engine.walpulse.infrastructure.adapter.in.rest;

import com.engine.walpulse.application.service.SchemaCacheService;
import com.engine.walpulse.domain.model.TableMetadata;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * REST controller for inspecting the Schema Registry and schema evolution log.
 */
@RestController
@RequestMapping("/api/v1/schemas")
public class SchemaController {

    private final SchemaCacheService schemaCache;

    public SchemaController(SchemaCacheService schemaCache) {
        this.schemaCache = schemaCache;
    }

    @GetMapping
    public ResponseEntity<List<TableMetadata>> getSchemas() {
        return ResponseEntity.ok(schemaCache.getAllRelations());
    }

    @GetMapping("/history")
    public ResponseEntity<List<SchemaCacheService.SchemaChangeEntry>> getEvolutionHistory() {
        return ResponseEntity.ok(schemaCache.getEvolutionLog());
    }
}
