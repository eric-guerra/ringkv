package com.ericguerra.ringkv.protocol;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class CodecTest {
    private static Message roundTrip(Message m) {
        return Codec.decode(Codec.encode(m));
    }

    @Test
    void roundTripsEveryMessage() {
        byte[] bin = {0, 1, 2, (byte) 255, '\n', ' '};
        assertEquals(new Message.Get("a"), roundTrip(new Message.Get("a")));
        assertEquals(new Message.Delete("a"), roundTrip(new Message.Delete("a")));
        assertArrayEquals(bin, ((Message.Put) roundTrip(new Message.Put("k", bin))).value());

        var rput = (Message.ReplicaPut) roundTrip(new Message.ReplicaPut("k", bin, 123L, "n1"));
        assertArrayEquals(bin, rput.value());
        assertEquals(123L, rput.timestamp());
        assertEquals("n1", rput.writer());

        var tomb = (Message.ReplicaPut) roundTrip(new Message.ReplicaPut("k", null, 5L, "n2"));
        assertNull(tomb.value());

        var val = (Message.Value) roundTrip(new Message.Value(true, "hi".getBytes(StandardCharsets.UTF_8), 9, "n3"));
        assertTrue(val.found());
        assertEquals("hi", new String(val.value(), StandardCharsets.UTF_8));
        assertFalse(((Message.Value) roundTrip(new Message.Value(false, null, 0, ""))).found());

        var err = (Message.Error) roundTrip(new Message.Error("UNAVAILABLE", "only 1 healthy\nreplica"));
        assertEquals("UNAVAILABLE", err.code());
        assertEquals("only 1 healthy replica", err.message());
    }

    @Test
    void commandsAreCaseInsensitive() {
        assertEquals(new Message.Get("Key"), Codec.decode("get Key"));
    }

    @Test
    void rejectsMalformedInput() {
        assertThrows(Codec.ProtocolException.class, () -> Codec.decode(""));
        assertThrows(Codec.ProtocolException.class, () -> Codec.decode("FROB x"));
        assertThrows(Codec.ProtocolException.class, () -> Codec.decode("GET"));
        assertThrows(Codec.ProtocolException.class, () -> Codec.decode("GET a b"));
        assertThrows(Codec.ProtocolException.class, () -> Codec.decode("PUT k not*base64"));
        assertThrows(Codec.ProtocolException.class, () -> Codec.decode("RPUT k notanumber n1 -"));
        assertThrows(Codec.ProtocolException.class, () -> Codec.decode("GET " + "x".repeat(300)));
    }
}
