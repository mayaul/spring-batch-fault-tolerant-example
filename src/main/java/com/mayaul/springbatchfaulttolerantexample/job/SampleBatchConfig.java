package com.mayaul.springbatchfaulttolerantexample.job;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.stream.LongStream;

import static java.util.stream.Collectors.toList;

@Slf4j
@Configuration
public class SampleBatchConfig {

    private static final int RUNTIME_EXCEPTION_SKIP_LIMIT = 3;
    private static final int RUNTIME_EXCEPTION_LIMIT_LIMIT = 2;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public SampleBatchConfig(JobBuilderFactory jobBuilderFactory,
                             StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public Job sampleJob() {
        return jobBuilderFactory.get("sample job")
                .start(sampleStep())
                .build();
    }

    @Bean
    @JobScope
    public Step sampleStep() {
        log.info("create step");
        return stepBuilderFactory.get("sample step")
                .<Long, Long>chunk(5)
                .faultTolerant()
                .skip(RuntimeException.class)
                .skipLimit(RUNTIME_EXCEPTION_SKIP_LIMIT)
                .retry(RuntimeException.class)
                .retryLimit(RUNTIME_EXCEPTION_LIMIT_LIMIT)
                .listener(sampleBatchListener())
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .transactionManager(new ResourcelessTransactionManager())
                .build();

    }

    @Bean
    @StepScope
    public SampleBatchListener sampleBatchListener() {
        return new SampleBatchListener();
    }

    @Bean
    @StepScope
    public ListItemReader<Long> reader() {
        return new ListItemReader<>(LongStream.rangeClosed(1L, 20L).boxed().collect(toList()));
    }

    @Bean
    @StepScope
    public ItemProcessor<Long, Long> processor() {
        return item -> {
            log.info("[PROCESSOR] item id: {}", item);
            if (item == 5 || item == 10) {
                throw new IllegalStateException("item id: " + item);
            }
            return item;
        };
    }

    @Bean
    @StepScope
    public ItemWriter<Long> writer() {
        return items -> log.info("[WRITER] items: {}", items);
    }
}
