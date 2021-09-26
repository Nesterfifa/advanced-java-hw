package info.kgeorgiy.ja.nesterenko.walk;

import java.io.*;
import java.nio.file.*;

public class RecursiveWalk extends AbstractWalk {
    public RecursiveWalk(String[] args) throws WalkException {
        super(args);
    }

    public static void main(String[] args) {
        try {
            Walker walker = new RecursiveWalk(args);
            walker.process();
        } catch (WalkException e) {
            System.err.println(e.getMessage());
        }
    }

    protected void walkPath(String file, Writer writer) throws WalkException {
        try {
            try {
                FileVisitor<Path> visitor = new WalkFileVisitor(writer);
                Files.walkFileTree(Path.of(file), visitor);
            } catch (InvalidPathException e) {
                writer.write(String.format("%016x %s%n", 0, file));
            }
        } catch (IOException e) {
            throw new WalkException("Error while writing to output file: " + e.getMessage());
        }
    }
}
