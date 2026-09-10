package com.engine.walpulse.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

public class ArchUnitTest {

    private final JavaClasses classes = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("com.engine.walpulse");

    @Test
    @DisplayName("Domain layer must have zero dependencies on Spring, Jackson, Postgres or Infrastructure")
    void domainMustBePure() {
        noClasses().that().resideInAPackage("..walpulse.domain..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "org.springframework..",
                        "com.fasterxml.jackson..",
                        "org.postgresql..",
                        "org.apache.kafka..",
                        "..walpulse.infrastructure..",
                        "..walpulse.application.."
                )
                .because("Hexagonal Architecture requires the Domain core to be completely framework-independent")
                .check(classes);
    }

    @Test
    @DisplayName("Application layer must not depend on Infrastructure adapters")
    void applicationMustNotDependOnInfrastructure() {
        noClasses().that().resideInAPackage("..walpulse.application..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "..walpulse.infrastructure.adapter.."
                )
                .because("Application services must communicate with outside world only through domain ports")
                .check(classes);
    }
}
