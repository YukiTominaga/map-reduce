package dev.tominaga.beam.examples;

import dev.tominaga.beam.examples.interfaces.MyOption;
import dev.tominaga.beam.examples.transform.FilterByWord;
import dev.tominaga.beam.examples.transform.KVtoString;
import dev.tominaga.beam.examples.transform.OutputPackage;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.values.KV;

public class MapReduce {
    public static void main(String[] args) {
        MyOption options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOption.class);
        Pipeline pipeline = Pipeline.create(options);

        String inputFiles = options.getInput() + "*.java";
        String outputPrefix = options.getOutputPrefix();

        pipeline
            .apply("GetJavaFiles", TextIO.read().from(inputFiles))
            .apply("FilterByWord", ParDo.of(new FilterByWord("import")))
            .apply("OutputPackage", ParDo.of(new OutputPackage()))
            .apply(Sum.integersPerKey())
            .apply("Top_5", Top.of(5, new KV.OrderByValue<>()))
            .apply("ToString", ParDo.of(new KVtoString()))
            .apply(TextIO.write().to(outputPrefix).withSuffix(".csv").withoutSharding());

        pipeline.run().waitUntilFinish();
    }
}
