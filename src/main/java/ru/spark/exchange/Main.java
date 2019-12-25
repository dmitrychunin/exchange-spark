package ru.spark.exchange;

import lombok.extern.slf4j.Slf4j;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketListener;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;

import java.util.concurrent.ExecutionException;

@Slf4j
public class Main {
//todo кешировать результат в ConcurrentNavigableMap: ключ quantity (количество) и значение цена (последняя пришедшая)
//todo ограничить размер фрейма, до этого запускать:-Dorg.asynchttpclient.webSocketMaxFrameSize=65536
    private static final Producer producer = new Producer();
    private static WebSocket webSocket;

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        WebSocketUpgradeHandler.Builder upgradeHandlerBuilder
                = new WebSocketUpgradeHandler.Builder();
        WebSocketUpgradeHandler wsHandler = upgradeHandlerBuilder
                .addWebSocketListener(new WebSocketListener() {
                    @Override
                    public void onClose(WebSocket websocket, int code, String reason) {
                        // WebSocket connection closed
                        log.info("ws closed");
                        websocket.sendPongFrame();
                    }

                    @Override
                    public void onError(Throwable t) {
                        // WebSocket connection error
                        log.info("ws error");
                        t.printStackTrace();
                    }


                    @Override
                    public void onBinaryFrame(byte[] payload, boolean finalFragment, int rsv) {
                        log.info("binary frame {}", payload);
                    }

                    @Override
                    public void onTextFrame(String payload, boolean finalFragment, int rsv) {
//                        todo отбрасывать первое сообщение типа {"result":null,"id":1}
                        producer.send(KafkaTopic.ORDER, payload);
                    }

                    @Override
                    public void onPingFrame(byte[] payload) {
                        log.info("ping frame");
                        webSocket.sendPongFrame(payload);
                    }

                    @Override
                    public void onPongFrame(byte[] payload) {
                        log.info("pong frame");
                    }

                    @Override
                    public void onOpen(WebSocket websocket) {
                        // WebSocket connection opened
                        log.info("ws opened");
                        websocket.sendTextFrame("{\n" +
                                "  \"method\": \"SUBSCRIBE\",\n" +
                                "  \"params\": [\n" +
                                "    \"btcusdt@depth\"\n" +
                                "  ],\n" +
                                "  \"id\": 1\n" +
                                "}");
                    }
                }).build();

        webSocket = Dsl.asyncHttpClient()
                .prepareGet("wss://stream.binance.com:9443/ws")
                .setRequestTimeout(5000)
                .execute(wsHandler)
                .get();
    }
}
