package com.xiaozhu.data.event;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;

public class TextSource implements SourceFunction<String> {

    private String filepath;

    private boolean canceled = false;

    public TextSource(String filepath) {
        this.filepath = filepath;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(filepath));
        while (!canceled && reader.ready()) {
            String line = reader.readLine();
            sourceContext.collect(line);
            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        canceled = true;
    }
}
