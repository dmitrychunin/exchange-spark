package ru.spark.exchange;

import lombok.SneakyThrows;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import ru.spark.exchange.consume.OrdersConsumer;

public class QuartzTrigger {
    @SneakyThrows
    public static void main(String[] args) {
        // trigger runs every second
        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("sparkJob1Trigger", "sparkJobsGroup")
                .withSchedule(
                        CronScheduleBuilder.cronSchedule("* * * * * ?"))
                .build();


        JobDetail sparkQuartzJob = JobBuilder.newJob(SparkLauncherQuartzJob.class)
                .withIdentity("SparkLauncherQuartzJob", "sparkJobsGroup")
                .build();

        Scheduler scheduler = new StdSchedulerFactory().getScheduler();
        scheduler.start();
        scheduler.scheduleJob(sparkQuartzJob , trigger);

        OrdersConsumer.start();
    }
}
