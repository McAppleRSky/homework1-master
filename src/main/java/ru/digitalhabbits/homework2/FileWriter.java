package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Exchanger;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public abstract class FileWriter implements Runnable {

    private static final Logger logger = getLogger(FileWriter.class);

    private String resultFileName;
    private Exchanger<ConcurrentHashMap<Integer, Pair<String, Integer>>> writeExchanger;

    public abstract void init();

    public void setResultFileName(String resultFileName) {
        this.resultFileName = resultFileName;
    }
    public void setWriteExchanger(Exchanger<ConcurrentHashMap<Integer, Pair<String, Integer>>> writeExchanger) {
        this.writeExchanger = writeExchanger;
    }

    public FileWriter() {
        init();
    }

    @Override
    public void run() {
        logger.info("Started writer thread {}", currentThread().getName());
        PrintWriter writer;
        try {
            writer = new PrintWriter(resultFileName, "UTF-8");
            boolean conditionContinueWrite = true;
            while(conditionContinueWrite){
                ConcurrentHashMap<Integer, Pair<String, Integer>> writeBuffer = writeExchanger.exchange(null);
                if (writeBuffer != null) {
                    if (!writeBuffer.isEmpty()) {
                        for (int i = 0; i < writeBuffer.size(); i++) {
                            Pair<String, Integer> pair = writeBuffer.get(i);
                            writer.println(pair.getLeft() + " " + pair.getRight());
                        }
                    }
                } else {
                    conditionContinueWrite = false;
                }
            }
            writer.close();
        } catch (InterruptedException e) {
            logger.error("Ops!", e);
        } catch (FileNotFoundException e) {
            logger.error("Ops!", e);
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Finish writer thread {}", currentThread().getName());
    }

}
