package info.kgeorgiy.ja.nesterenko.walk;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileHasher {
    public static long getFileHash(Path file) {
        long hash = 0;
        byte[] bytes = new byte[1024];
        try (InputStream reader = new BufferedInputStream(Files.newInputStream(file))) {
            int c;
            while ((c = reader.read(bytes)) >= 0) {
                for (int i = 0; i < c; i++) {
                    hash = (hash << 8) + (bytes[i] & 0xff);
                    long high = hash & 0xff00_0000_0000_0000L;
                    if (high != 0) {
                        hash ^= high >> 48;
                        hash &= ~high;
                    }
                }
            }
            return hash;
        } catch (IOException e) {
            return 0;
        }
    };
}
