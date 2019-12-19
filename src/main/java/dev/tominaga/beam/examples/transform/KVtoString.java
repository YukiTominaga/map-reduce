package dev.tominaga.beam.examples.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.List;

public class KVtoString extends DoFn<List<KV<String, Integer>>, String> {
    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
        StringBuilder stringBuilder = new StringBuilder();
        for (KV<String, Integer> kv : context.element()) {
            stringBuilder.append(kv.getKey()).append(",").append(kv.getValue()).append("\n");
        }
        context.output(stringBuilder.toString());
    }
}
