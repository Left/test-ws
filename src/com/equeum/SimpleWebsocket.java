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
        String action;
        double vol;
        double close;
        long time;

        public SymData(String action, double vol, double close) {
            this.action = action;
            this.vol = vol;
            this.close = close;
            this.time = System.currentTimeMillis();
        }
    }

    public static void main(String[] args) throws InterruptedException {

        final HttpClient httpClient = HttpClient.newHttpClient();
        final String url = "wss://streamer.cryptocompare.com/v2?api_key=" + "437a8eeb9e2dac438219fd9606a8493716d67a40f08f1656c26f0b0aa0e4b13a";

        Gson gson = new GsonBuilder().create();

        Map<String, Map<Long, List<SymData>>> syms = new TreeMap<String, Map<Long, List<SymData>>>();

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
                            syms.put(sym, new TreeMap<Long, List<SymData>>());
                        }

                        Map<Long, List<SymData>> hm = syms.get(sym);

                        long ts = js.get("TS").getAsLong();

                        if (startCollecting < ts) {
                            if (!hm.containsKey(ts)) {
                                hm.put(ts, new ArrayList<SymData>());
                            }

                            hm.get(ts).add(new SymData(
                                    js.get("ACTION").getAsString(),
                                    js.get("VOLUMETO").getAsDouble(),
                                    js.get("CLOSE").getAsDouble()
                            ));
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

        String tickersList = Arrays.stream(tickers).map(t -> "\"24~CCCAGG~" + t + "~USD~m\"").collect(Collectors.joining(", "));
        String json = "{\"action\":\"SubAdd\", \"subs\":[" + tickersList + "]}";
        websocket.sendText(json, true);

        for (;;) {
            System.out.println("======== START DUMP ===========");
            syms.entrySet().stream().forEach(e -> {
                String sym = e.getKey();
                Map<Long, List<SymData>> map = e.getValue();

                System.out.println(sym + ":");
                map.entrySet().stream().forEach( t -> {
                    // if (t.getValue().volReceived == 0) {
                    long start = (t.getValue().size() > 0) ? t.getValue().get(0).time : 0;
                    System.out.println( "\t" + t.getKey() + " : " +
                            t.getValue().stream().map(a -> "[" + a.action + " (" + (a.time - start)/1000.0 + ") " + a.close + " " + a.vol + "]").collect(Collectors.joining(" ")));
                    // }
                });
            });
            System.out.println("======== END DUMP ===========");

            Thread.sleep(20000);
        }
    }
}
