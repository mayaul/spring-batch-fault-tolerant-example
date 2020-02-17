package com.mayaul.springbatchfaulttolerantexample.job;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.listener.SkipListenerSupport;

@Slf4j
public class SampleBatchListener extends SkipListenerSupport<Long, Long> {

    @Override
    public void onSkipInRead(Throwable t) {
        log.error("reader error", t);
    }

    @Override
    public void onSkipInProcess(Long data,
                                Throwable t) {
        log.error("processor error data: {}", data, t);
    }

    @Override
    public void onSkipInWrite(Long data,
                              Throwable t) {
        log.error("writer error data: {}", data, t);
    }
}
