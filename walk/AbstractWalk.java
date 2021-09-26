package info.kgeorgiy.ja.nesterenko.walk;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;

public abstract class AbstractWalk implements Walker {
    private final Path inputPath;
    private final Path outputPath;

    public AbstractWalk(String[] args) throws WalkException {
        checkArgs(args);
        inputPath = stringToPath(args[0]);
        outputPath = stringToPath(args[1]);
        createOutputDirectories(outputPath);
    }

    protected void createOutputDirectories(Path file) throws WalkException {
        if (file.getParent() != null && Files.notExists(file.getParent())) {
            try {
                Files.createDirectories(file.getParent());
            } catch (IOException e) {
                throw new WalkException("Couldn't create output file directory: " + e.getMessage() + file);
            }
        }
    }

    protected void checkArgs(String[] args) throws WalkException {
        if (args == null || args.length != 2 || args[0] == null || args[1] == null) {
            throw new WalkException("Expected two not null args in input");
        }
    }

    protected Path stringToPath(String path) throws WalkException {
        try {
            return Path.of(path);
        } catch (InvalidPathException e) {
            throw new WalkException("Invalid path: " + e.getMessage());
        }
    }

    abstract void walkPath(String file, Writer writer) throws WalkException;

    public void process() throws WalkException {
        try (BufferedReader reader = Files.newBufferedReader(inputPath)) {
            try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
                try {
                    String file;
                    while ((file = reader.readLine()) != null) {
                        walkPath(file, writer);
                    }
                } catch (IOException e) {
                    throw new WalkException("Input file reading error: " + e.getMessage());
                }
            } catch (IOException e) {
                throw new WalkException("Output file error: " + e.getMessage());
            }
        } catch (IOException e) {
            throw new WalkException("Input file error: " + e.getMessage());
        }
    }
}
