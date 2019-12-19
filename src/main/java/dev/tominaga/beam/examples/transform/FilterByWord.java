package dev.tominaga.beam.examples.transform;

import org.apache.beam.sdk.transforms.DoFn;

public class FilterByWord extends DoFn<String, String> {
    String keyword;

    public FilterByWord(String keyword) {
        this.keyword = keyword;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
        String line = context.element();
        if (line.startsWith(keyword)) {
            context.output(line);
        }
    }
}
