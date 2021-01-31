package com.appsflyer.AerospikeClj;

import java.util.UUID;

public class ByteUtils {

    public static UUID uuidFromBytes(byte[] data) {
        long msb = 0;
        long lsb = 0;

        for (int i=0; i<8; i++) {
            msb = (msb << 8) | (data[i] & 0xff);
        }

        for (int i=8; i<16; i++) {
            lsb = (lsb << 8) | (data[i] & 0xff);
        }

        return new UUID(msb, lsb);
    }

    public static byte[] bytesFromUUID(UUID uuid) {
        long lsb = uuid.getLeastSignificantBits();
        long msb = uuid.getMostSignificantBits();

        byte[] data = new byte[16];
        for (int i = 7; i >= 0; i--) {
            data[i] = (byte)(msb & 0xff);
            msb = msb >> 8;
        }

        for (int i=15; i >= 8; i--) {
            data[i] = (byte)(lsb & 0xff);
            lsb = lsb >> 8;
        }

        return data;
    }
}
