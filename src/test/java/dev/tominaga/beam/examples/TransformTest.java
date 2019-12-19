package dev.tominaga.beam.examples;

import dev.tominaga.beam.examples.transform.FilterByWord;
import dev.tominaga.beam.examples.transform.OutputPackage;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;

public class TransformTest {
    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    private static final String[] IMPORT_CASE_ARR = new String[]{"import hoge", "fuga", "import bar"};
    private static final List<String> IMPORT_CASE_LIST = Arrays.asList(IMPORT_CASE_ARR);

    private static final String[] PACKAGE_CASE_ARR = new String[]{"import hoge", "import bar"};
    private static final List<String> PACKAGE_CASE_LIST = Arrays.asList(PACKAGE_CASE_ARR);

    private static final List<KV<String, Integer>> KV_ARRAY_LIST = Arrays.asList(KV.of("hoge", 1), KV.of("bar", 1));

    @Test
    @Category(NeedsRunner.class)
    public void doFilterTest() {
        PCollection<String> input = pipeline.apply(Create.of(IMPORT_CASE_LIST).withCoder(StringUtf8Coder.of()));
        PCollection<String> case1 = input.apply(ParDo.of(new FilterByWord("import")));
        PAssert.that(case1).containsInAnyOrder("import hoge", "import bar");
        pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void doOutputPackageTest() {
        PCollection<String> input = pipeline.apply(Create.of(PACKAGE_CASE_LIST).withCoder(StringUtf8Coder.of()));
        PCollection<KV<String, Integer>> case1 = input.apply(ParDo.of(new OutputPackage()));
        PAssert.that(case1).containsInAnyOrder(KV.of("hoge", 1), KV.of("bar", 1));
        pipeline.run();
    }
}
