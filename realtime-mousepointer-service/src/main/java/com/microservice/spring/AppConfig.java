package com.microservice.spring;

import java.util.concurrent.Executor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@Configuration
@EnableScheduling
@EnableAsync(proxyTargetClass=true)
@EnableWebMvc
public class AppConfig implements AsyncConfigurer {
	/**
	 * Max idle threads allowed in thread pools
	 */
	private static final int MAX_IDLE_THREAD_COUNT = 50;

	/**
	 * Custom thread pool task executor for @Async tasks
	 */
	@Override
	@Bean(destroyMethod = "shutdown", name="microserviceTaskExecutor")
    public Executor getAsyncExecutor() {
//        int availableCores = Runtime.getRuntime().availableProcessors();

		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
//        executor.setMaxPoolSize(4);//	Commented out because we allow however many threads to be running at the same time. We ideally don't create that many
//        executor.setQueueCapacity(0);//	same as above
        executor.setThreadNamePrefix("microserviceTaskExecutor-");
        executor.initialize();
        return executor;
    }
	
	/**
	 * Custom config for task scheduler for handling @Scheduled tasks
	 * @return
	 */
	@Bean
    public TaskScheduler taskScheduler() {
        int availableCores = Runtime.getRuntime().availableProcessors();
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(Math.min(MAX_IDLE_THREAD_COUNT, availableCores));
        scheduler.setThreadNamePrefix("microserviceTaskScheduler-");
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        return scheduler;
    }
}
