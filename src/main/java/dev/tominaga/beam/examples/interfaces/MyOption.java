package dev.tominaga.beam.examples.interfaces;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface MyOption extends PipelineOptions {
    @Description("Output prefix")
    @Default.String("/tmp/output")
    String getOutputPrefix();

    void setOutputPrefix(String s);

    @Description("Input directory")
    @Default.String("gs://ca-tominaga-test-training/java/")
    String getInput();

    void setInput(String s);
}
