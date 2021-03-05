package io.joyrpc.example.boot;

/*-
 * #%L
 * joyrpc
 * %%
 * Copyright (C) 2019 joyrpc.io
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.joyrpc.example.service.DemoServiceGrpc;
import io.joyrpc.example.service.HelloRequest;
import io.joyrpc.example.service.HelloResponse;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 客户端
 */
@SpringBootApplication
public class BootGrpcClient {

    public static void main(String[] args) {
        ConfigurableApplicationContext run = SpringApplication.run(BootGrpcClient.class, args);
        DemoServiceClient client = new DemoServiceClient("127.0.0.1", 22000);
        AtomicLong counter = new AtomicLong(0);
        while (true) {
            try {
                String hello = client.sayHello(String.valueOf(counter.incrementAndGet()));
                System.out.println(hello);
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                break;
            } catch (Throwable e) {
                System.out.println(e.getMessage());
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException ex) {
                }
            }
        }
        client.shutdown();
    }

    public static class DemoServiceClient {
        private final ManagedChannel channel;
        private final DemoServiceGrpc.DemoServiceBlockingStub demoServiceStub;

        public DemoServiceClient(String host, int port) {
            channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
            demoServiceStub = DemoServiceGrpc.newBlockingStub(channel);
        }

        public void shutdown() {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            }
        }

        public String sayHello(String name) {
            HelloRequest request = HelloRequest.newBuilder().setName(name).build();
            HelloResponse response;
            response = demoServiceStub.sayHello(request);
            return response.getMessage();
        }
    }

}

