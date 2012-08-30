package com.ericguerra.ringkv.protocol;

import java.util.Base64;

/**
 * Line-oriented text protocol, one message per line, fields separated by single spaces.
 * Keys must not contain whitespace; values are Base64 so arbitrary bytes are safe.
 *
 * <pre>
 *   GET key                   PUT key b64             DEL key
 *   RGET key                  RPUT key ts writer b64|-
 *   PING from                 PONG from
 *   OK                        VAL b64|- ts writer     NIL
 *   ERR CODE message...
 * </pre>
 */
public final class Codec {
    private static final Base64.Encoder B64 = Base64.getEncoder();
    private static final Base64.Decoder B64D = Base64.getDecoder();
    public static final int MAX_KEY = 256;

    private Codec() {}

    public static String encode(Message m) {
        return switch (m) {
            case Message.Get g -> "GET " + g.key();
            case Message.Put p -> "PUT " + p.key() + " " + B64.encodeToString(p.value());
            case Message.Delete d -> "DEL " + d.key();
            case Message.ReplicaGet g -> "RGET " + g.key();
            case Message.ReplicaPut p -> "RPUT " + p.key() + " " + p.timestamp() + " " + p.writer() + " " + b64OrDash(p.value());
            case Message.Ping p -> "PING " + p.from();
            case Message.Pong p -> "PONG " + p.from();
            case Message.Ok ok -> "OK";
            case Message.Value v -> v.found() ? "VAL " + b64OrDash(v.value()) + " " + v.timestamp() + " " + v.writer() : "NIL";
            case Message.Error e -> "ERR " + e.code() + " " + e.message().replace('\n', ' ');
        };
    }

    public static Message decode(String line) {
        String[] parts = line.trim().split(" ", -1);
        if (parts.length == 0 || parts[0].isEmpty()) throw new ProtocolException("empty line");
        String cmd = parts[0].toUpperCase();
        try {
            return switch (cmd) {
                case "GET" -> new Message.Get(key(parts, 2));
                case "PUT" -> new Message.Put(key(parts, 3), B64D.decode(parts[2]));
                case "DEL" -> new Message.Delete(key(parts, 2));
                case "RGET" -> new Message.ReplicaGet(key(parts, 2));
                case "RPUT" -> new Message.ReplicaPut(key(parts, 5), dashOrB64(parts[4]), Long.parseLong(parts[2]), parts[3]);
                case "PING" -> new Message.Ping(arity(parts, 2)[1]);
                case "PONG" -> new Message.Pong(arity(parts, 2)[1]);
                case "OK" -> new Message.Ok();
                case "NIL" -> new Message.Value(false, null, 0, "");
                case "VAL" -> new Message.Value(true, dashOrB64(arity(parts, 4)[1]), Long.parseLong(parts[2]), parts[3]);
                case "ERR" -> new Message.Error(parts.length > 1 ? parts[1] : "UNKNOWN",
                        parts.length > 2 ? String.join(" ", java.util.Arrays.copyOfRange(parts, 2, parts.length)) : "");
                default -> throw new ProtocolException("unknown command " + cmd);
            };
        } catch (IllegalArgumentException e) {
            if (e instanceof ProtocolException pe) throw pe;
            throw new ProtocolException("malformed " + cmd + ": " + e.getMessage());
        }
    }

    private static String[] arity(String[] parts, int n) {
        if (parts.length != n) throw new ProtocolException(parts[0] + " expects " + (n - 1) + " argument(s)");
        return parts;
    }

    private static String key(String[] parts, int n) {
        String k = arity(parts, n)[1];
        if (k.isEmpty() || k.length() > MAX_KEY) throw new ProtocolException("key must be 1-" + MAX_KEY + " chars");
        return k;
    }

    private static String b64OrDash(byte[] v) {
        return v == null ? "-" : B64.encodeToString(v);
    }

    private static byte[] dashOrB64(String s) {
        return "-".equals(s) ? null : B64D.decode(s);
    }

    public static final class ProtocolException extends IllegalArgumentException {
        public ProtocolException(String msg) {
            super(msg);
        }
    }
}
