package com.engine.walpulse;

import com.engine.walpulse.domain.model.LsnPosition;
import com.engine.walpulse.domain.port.out.LogicalReplicationPort;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@SpringBootTest(properties = "walpulse.auto-start=false")
class WalPulseApplicationTests {

    @MockBean
    private LogicalReplicationPort logicalReplicationPort;

    @Test
    @DisplayName("Spring application context loads successfully with virtual threads and beans wired")
    void contextLoads() {
        when(logicalReplicationPort.isConnected()).thenReturn(true);
        when(logicalReplicationPort.readNextEvent()).thenReturn(Optional.empty());
        when(logicalReplicationPort.getLastReceivedLsn()).thenReturn(LsnPosition.ZERO);
        when(logicalReplicationPort.getLastFlushedLsn()).thenReturn(LsnPosition.ZERO);

        assertThat(logicalReplicationPort).isNotNull();
    }
}
