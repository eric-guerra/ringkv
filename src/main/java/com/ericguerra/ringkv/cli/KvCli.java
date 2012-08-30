package com.ericguerra.ringkv.cli;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Tiny interactive client. Accepts friendly commands and translates to the wire protocol:
 * <pre>
 *   get user:42
 *   put user:42 {"name":"Ada"}
 *   del user:42
 * </pre>
 */
public final class KvCli {
    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 7000;
        try (Socket socket = new Socket(host, port);
             var in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             var out = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
             var console = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            System.out.println("connected to " + host + ":" + port + " (get/put/del, ctrl-d to quit)");
            String line;
            while (true) {
                System.out.print("ringkv> ");
                if ((line = console.readLine()) == null) break;
                String[] parts = line.trim().split("\\s+", 3);
                if (parts[0].isEmpty()) continue;
                String wire = switch (parts[0].toLowerCase()) {
                    case "get" -> parts.length == 2 ? "GET " + parts[1] : null;
                    case "del" -> parts.length == 2 ? "DEL " + parts[1] : null;
                    case "put" -> parts.length == 3
                            ? "PUT " + parts[1] + " " + Base64.getEncoder().encodeToString(parts[2].getBytes(StandardCharsets.UTF_8))
                            : null;
                    default -> null;
                };
                if (wire == null) {
                    System.out.println("usage: get <key> | put <key> <value> | del <key>");
                    continue;
                }
                out.println(wire);
                System.out.println(pretty(in.readLine()));
            }
        }
    }

    static String pretty(String reply) {
        if (reply == null) return "(connection closed)";
        if (reply.startsWith("VAL ")) {
            String[] p = reply.split(" ");
            return new String(Base64.getDecoder().decode(p[1]), StandardCharsets.UTF_8) + "   (ts=" + p[2] + ", writer=" + p[3] + ")";
        }
        return reply.equals("NIL") ? "(nil)" : reply;
    }
}
