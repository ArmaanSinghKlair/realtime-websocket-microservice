package com.microservice.spring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.http.CacheControl;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.view.InternalResourceViewResolver;
import org.springframework.web.servlet.view.JstlView;

@Configuration
@EnableWebMvc
public class WebConfig implements WebMvcConfigurer{
	@Autowired
	ApplicationContext applicationContext;
	@Autowired
	private Environment env;
	
	private static int STATIC_RESOURCE_CACHE_MINS = 10;
	
	// for direct jsp view resolver
	@Bean
	public InternalResourceViewResolver internalResolver() {
		InternalResourceViewResolver resolver = new InternalResourceViewResolver();
		resolver.setViewClass(JstlView.class);
		resolver.setPrefix("/WEB-INF/");
		resolver.setSuffix(".jsp");
		resolver.setOrder(1);
		resolver.setCacheLimit(0);
		return resolver;
	}

    @Override
    public void addResourceHandlers(final ResourceHandlerRegistry registry) {
    	ResourceHandlerRegistration reg = registry.addResourceHandler("/**").addResourceLocations("classpath:/");
    	if(env.getProperty(PropertyConfig.PROPERTY_KEY_ENV_NAME).equals(PropertyConfig.Properties.EnvironmentName.DEV.getValue())) {
    		reg.setCacheControl(CacheControl.noStore())
			.setCacheControl(CacheControl.noCache())
			.setCachePeriod(0);
    	} else {
    		reg.setCachePeriod(STATIC_RESOURCE_CACHE_MINS * 60);
    	}
    }
}
