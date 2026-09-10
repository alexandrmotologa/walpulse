package com.engine.walpulse.domain;

import com.engine.walpulse.domain.model.LsnPosition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LsnPositionTest {

    @Test
    @DisplayName("Should correctly parse standard hexadecimal LSN strings")
    void testParseLsn() {
        LsnPosition lsn = LsnPosition.valueOf("0/16B4F80");
        assertThat(lsn.asLong()).isEqualTo(0x16B4F80L);
        assertThat(lsn.asString()).isEqualTo("0/16B4F80");

        LsnPosition highLsn = LsnPosition.valueOf("1/2");
        assertThat(highLsn.asLong()).isEqualTo((1L << 32) | 2L);
        assertThat(highLsn.asString()).isEqualTo("1/2");
    }

    @Test
    @DisplayName("Should compare LSN positions correctly")
    void testCompareLsn() {
        LsnPosition lsn1 = LsnPosition.valueOf("0/1000");
        LsnPosition lsn2 = LsnPosition.valueOf("0/2000");
        LsnPosition lsn3 = LsnPosition.valueOf("1/0");

        assertThat(lsn1).isLessThan(lsn2);
        assertThat(lsn2).isLessThan(lsn3);
        assertThat(lsn1.distanceTo(lsn2)).isEqualTo(0x1000L);
    }

    @Test
    @DisplayName("Should handle empty and zero LSN inputs")
    void testZeroLsn() {
        assertThat(LsnPosition.valueOf(null)).isEqualTo(LsnPosition.ZERO);
        assertThat(LsnPosition.valueOf("")).isEqualTo(LsnPosition.ZERO);
        assertThat(LsnPosition.valueOf("0/0")).isEqualTo(LsnPosition.ZERO);
    }
}
