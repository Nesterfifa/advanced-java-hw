package info.kgeorgiy.ja.nesterenko.walk;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;

public class Walk extends AbstractWalk {
    public Walk(String[] args) throws WalkException {
        super(args);
    }

    public static void main(String[] args) {
        try {
            Walker walker = new Walk(args);
            walker.process();
        } catch (WalkException e) {
            System.err.println(e.getMessage());
        }
    }

    protected void walkPath(String file, Writer writer) throws WalkException {
        try {
            try {
                if (Files.isDirectory(Path.of(file))) {
                    writer.write(String.format("%016x %s%n", 0, file));
                } else {
                    writer.write(String.format("%016x %s%n", FileHasher.getFileHash(Path.of(file)), file));
                }
            } catch (InvalidPathException e) {
                writer.write(String.format("%016x %s%n", 0, file));
            }
        } catch (IOException e) {
            throw new WalkException("Error while writing to output file: " + e.getMessage());
        }
    }
}
