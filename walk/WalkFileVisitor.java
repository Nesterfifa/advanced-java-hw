package info.kgeorgiy.ja.nesterenko.walk;

import java.io.*;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class WalkFileVisitor extends SimpleFileVisitor<Path> {
    private final Writer writer;

    public WalkFileVisitor(Writer writer) {
        this.writer = writer;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        writer.write(String.format("%016x %s%n", FileHasher.getFileHash(file), file.toString()));
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
        writer.write(String.format("%016x %s%n", 0, file.toString()));
        return FileVisitResult.CONTINUE;
    }
}
