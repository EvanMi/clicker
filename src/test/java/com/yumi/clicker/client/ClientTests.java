package com.yumi.clicker.client;

import com.yumi.clicker.dto.ClickCommand;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ClientTests {

    @Test
    void testClick() throws InterruptedException {
        RestTemplate restTemplate = new RestTemplate();
        String url = "http://127.0.0.1:8080/click";
        ExecutorService executorService = Executors.newFixedThreadPool(32);

        for (long i = 1; i < 32; i++) {
            long finalI = i;
            executorService.submit(() -> {
                ClickCommand clickCommand = new ClickCommand();
                clickCommand.setClickType(1);
                for (long j = 1; j <= 200; j++) {
                   long id = finalI * 1000 + j;
                   clickCommand.setClickerId(id);
                   for (long k = 20; k < 1000; k++) {
                       clickCommand.setReceiverId(k);
                       restTemplate.postForEntity(url, clickCommand, String.class);
                   }
                }

            });
        }

        TimeUnit.SECONDS.sleep(10000);
        executorService.shutdown();
    }


    @Test
    void testClickHot() throws InterruptedException {
        RestTemplate restTemplate = new RestTemplate();
        String url = "http://127.0.0.1:8080/click";
        ExecutorService executorService = Executors.newFixedThreadPool(32);

        for (long i = 1; i < 32; i++) {
            long finalI = i;
            executorService.submit(() -> {
                ClickCommand clickCommand = new ClickCommand();
                clickCommand.setClickType(1);
                clickCommand.setReceiverId(1L);
                for (long j = 1; j <= 900; j++) {
                    long id = finalI * 1000 + j;
                    clickCommand.setClickerId(id);
                    restTemplate.postForEntity(url, clickCommand, String.class);
                }

            });
        }

        TimeUnit.SECONDS.sleep(10000);
        executorService.shutdown();
    }

    @Test
    void testMassiveScan() throws Exception {
        RestTemplate restTemplate = new RestTemplate();
        String url = "http://127.0.0.1:8080/click";
        ExecutorService executorService = Executors.newFixedThreadPool(32);

        for (long i = 1; i < 32; i++) {
            long finalI = i;
            executorService.submit(() -> {
                ClickCommand clickCommand = new ClickCommand();
                clickCommand.setClickType(1);
                clickCommand.setClickerId(1L);
                for (long j = 1; j <= 900; j++) {
                    long id = finalI * 1000 + j;
                    clickCommand.setReceiverId(id);
                    restTemplate.postForEntity(url, clickCommand, String.class);
                }

            });
        }

        TimeUnit.SECONDS.sleep(10000);
        executorService.shutdown();
    }


    @Test
    void testGet() throws Exception {
        String urlTemplate = "http://127.0.0.1:8080/click/%s/%s";
        RestTemplate restTemplate = new RestTemplate();

        ExecutorService executorService = Executors.newFixedThreadPool(32);

        for (long i = 1; i < 32; i++) {
            long finalI = i;
            executorService.submit(() -> {
                for (long j = 1; j <= 900; j++) {
                    String url = String.format(urlTemplate, finalI, j);
                    restTemplate.getForEntity(url, String.class);
                }
            });
        }

        TimeUnit.SECONDS.sleep(10000);
        executorService.shutdown();
    }
}
