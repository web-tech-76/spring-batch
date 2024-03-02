package batch1.app;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.UUID;

@SpringBootApplication
public class Batch1Application {

    public static void main(String[] args) {
        SpringApplication.run(Batch1Application.class, args);
    }

    @Bean
    ApplicationRunner applicationRunner(JobLauncher jobLauncher, Job job) {
        return args -> {
            var jobParams = new JobParametersBuilder()
                    .addString("uuid", UUID.randomUUID().toString())
                    .toJobParameters();

            var executionHandler = jobLauncher.run(job, jobParams);
            var JobInstanceId = executionHandler.getJobInstance().getInstanceId();
            System.out.println("JobInstanceId = " + JobInstanceId);

        };
    }


    @Bean
    Job job(JobRepository jobRepository, CsvToDBStepConfiguration csvToDBStepConfiguration) {
        return new JobBuilder("job1", jobRepository)
                .incrementer(new RunIdIncrementer())//
                .start(csvToDBStepConfiguration.csvToDB())
                .build();
    }


}


