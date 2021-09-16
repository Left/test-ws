package com.equeum;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

/**
 * Java 14 and Google GSON lib are the only requirements
 */
public class SimpleWebsocket {
    static class SymData {
        String actionsReceived = "";
        double volReceived = Double.NaN;
    }

    public static void main(String[] args) throws InterruptedException {

        final HttpClient httpClient = HttpClient.newHttpClient();
        final String url = "wss://streamer.cryptocompare.com/v2?api_key=" + "437a8eeb9e2dac438219fd9606a8493716d67a40f08f1656c26f0b0aa0e4b13a";

        Gson gson = new GsonBuilder().create();

        Map<String, Map<Long, SymData>> syms = new TreeMap<String, Map<Long, SymData>>();

        long startCollecting = Instant.now().plusSeconds(15).toEpochMilli() / 1000;

        WebSocket websocket = httpClient.newWebSocketBuilder().buildAsync(URI.create(url), new WebSocket.Listener() {
            StringBuilder builder = new StringBuilder();

            @Override
            public CompletionStage<?> onText(WebSocket webSocket, CharSequence _data, boolean last) {
                builder.append(_data);
                if (last) {
                    String data = builder.toString();
                    builder = new StringBuilder();

                    System.out.println("-- " + data + " --");

                    JsonObject js = gson.fromJson(data.toString(), JsonObject.class);
                    if (js.has("TYPE") && Objects.equals(js.get("TYPE").getAsString(), "24")) {
                        String sym = js.get("FROMSYMBOL").getAsString();

                        if (!syms.containsKey(sym)) {
                            syms.put(sym, new TreeMap<Long, SymData>());
                        }

                        Map<Long, SymData> hm = syms.get(sym);

                        long ts = js.get("TS").getAsLong();

                        if (startCollecting < ts) {
                            if (!hm.containsKey(ts)) {
                                hm.put(ts, new SymData());
                            }

                            SymData symdata = hm.get(ts);

                            symdata.actionsReceived += js.get("ACTION").getAsString();
                            symdata.volReceived = js.get("VOLUMETO").getAsDouble();
                        }
                    }
                }

                return WebSocket.Listener.super.onText(webSocket, _data, last);
            }

            @Override
            public void onError(WebSocket webSocket, Throwable error) {
                System.out.println(error);
            }
        }).join();

        String[] tickers = new String[]{
            "1INCH", "AAVE", "ACH", "ADA", "ALGO", "AMP", "ANKR", "ATOM", "AXS", "BAL", "BAND", "BAT", "BCH", "BNT", "BOND", "BTC", "CELO", "CGLD",
            "CHZ", "CLV", "COMP", "COTI", "CRV", "CTSI", "DAI", "DASH", "DDX", "DOGE", "DOT", "ENJ", "EOS", "ETC", "ETH", "FARM", "FET", "FIL",
            "FORTH", "GRAPH", "GRT", "GTC", "ICP", "IOTX", "KEEP", "KNC", "LINK", "LPT", "LRC", "LTC", "MANA", "MASK", "MATIC", "MIR", "MKR",
            "MLN", "NKN", "NMR", "NU", "OGN", "OMG", "ORN", "OXT", "PAX", "PLAYD", "POLY", "QNT", "QUICK", "RAD", "RAI", "REN", "REP", "REQ",
            "RGT", "RLC", "RLY", "SHIB", "SKL", "SNX", "SOL", "STORJ", "SUSHI", "TRB", "TRIBE", "TRU", "UMA", "UNI", "USDT", "UST", "WBTC",
            "WLUNA", "XLM", "XRP", "XTZ", "XYO", "YFI", "YFII", "ZEC", "ZRX",
        };

        String tickersList = Arrays.stream(tickers).map(t -> "\"24~Coinbase~" + t + "~USD~m\"").collect(Collectors.joining(", "));
        String json = "{\"action\":\"SubAdd\", \"subs\":[" + tickersList + "]}";
        websocket.sendText(json, true);

        for (;;) {
            System.out.println("======== START DUMP ===========");
            syms.entrySet().stream().forEach(e -> {
                String sym = e.getKey();
                Map<Long, SymData> map = e.getValue();

                map.entrySet().stream().forEach( t -> {
                    // if (t.getValue().volReceived == 0) {
                        System.out.println(sym + "\t: " + t.getKey() + " : " + t.getValue().actionsReceived + " " + t.getValue().volReceived);
                    // }
                });
            });
            System.out.println("======== END DUMP ===========");

            Thread.sleep(20000);
        }
    }
}
