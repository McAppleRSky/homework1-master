package ru.digitalhabbits.homework2;

import org.slf4j.Logger;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter
        implements Runnable {
    private static final Logger logger = getLogger(FileWriter.class);

    @Override
    public void run() {
        logger.info("Started writer thread {}", currentThread().getName());

        logger.info("Finish writer thread {}", currentThread().getName());
    }
}
