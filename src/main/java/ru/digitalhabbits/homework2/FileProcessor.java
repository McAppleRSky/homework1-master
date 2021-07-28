package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    private static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();
    private final Exchanger<ConcurrentHashMap<Integer, Pair<String, Integer>>> writeExchanger = new Exchanger<>();
    private final ExecutorService lineCounterExecutorService = Executors.newFixedThreadPool(CHUNK_SIZE);
    private final Phaser phaser = new Phaser(1);
    private ConcurrentHashMap<Integer, Pair<String, Integer>> writeBuffer;

    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        logger.info("Start main thread {}", Thread.currentThread().getName());
        checkFileExists(processingFileName);
        checkFileNotExists(resultFileName);
        final File file = new File(processingFileName);

        // TODO: NotImplemented: запускаем FileWriter в отдельном потоке
        new Thread(new FileWriter() {
            @Override
            public void init() {
                setResultFileName(resultFileName);
                setWriteExchanger(writeExchanger);
            }
        }).start();
        try (final Scanner scanner = new Scanner(file, defaultCharset())) {
            while (scanner.hasNext()) {

                // TODO: NotImplemented: вычитываем CHUNK_SIZE строк для параллельной обработки
                List<String> lineList = new ArrayList<>();
                for (int i = 0; i < CHUNK_SIZE && scanner.hasNext(); i++) {
                    lineList.add(scanner.nextLine());
                }
                String[] lines = lineList.toArray(new String[lineList.size()]);

                // TODO: NotImplemented: обрабатывать строку с помощью LineProcessor. Каждый поток обрабатывает свою строку.
                writeBuffer = new ConcurrentHashMap<>();
                for (int i = 0; i < lines.length; i++) {
                    int finalI = i;
                    lineCounterExecutorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            phaser.register();
                            writeBuffer.put( finalI, new LineCounterProcessor().process(lines[finalI]) );
                            phaser.arriveAndAwaitAdvance();
                            phaser.arriveAndDeregister();
                        }
                    });
                }

                // TODO: NotImplemented: добавить обработанные данные в результирующий файл
                long counter = 0;
                while (writeBuffer.size() < lines.length) counter+=0.5;
                if(counter!=0) logger.warn("latency solve of phase : " + counter + " circles of counter");
                // TODO: try pass result through Observer pattern
                phaser.arriveAndAwaitAdvance();
                writeExchanger.exchange(writeBuffer);
            }

            // TODO: NotImplemented: остановить поток writerThread
            if(!scanner.hasNext()){
                writeExchanger.exchange(null);
            }
            writeBuffer = null;
            lineCounterExecutorService.shutdown();
            lineCounterExecutorService.awaitTermination(1, TimeUnit.HOURS);
        } catch (IOException e) {
            logger.error("Ops!", e);
        } catch (InterruptedException e) {
            logger.error("Ops!", e);
        }
        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private void checkFileNotExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (file.exists()) {
            if (file.isDirectory()) {
                throw new IllegalArgumentException("File '" + fileName + "' is directory");
            } else {
                file.delete();
            }
        }
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }

}
