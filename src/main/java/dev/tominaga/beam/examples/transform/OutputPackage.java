package dev.tominaga.beam.examples.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class OutputPackage extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public void processElement (ProcessContext context) throws Exception {
        String line = context.element();
        String withoutImport = line.replace("import ", "");
        String withoutSemicolon = withoutImport.replace(";", "");
        context.output(KV.of(withoutSemicolon, 1));
    }
}
