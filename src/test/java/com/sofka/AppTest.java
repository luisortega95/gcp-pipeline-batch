package com.sofka;

import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest {

   static final String line = "test:test:test:testing in progress:testing in progress:testing completed:done";

    @Test
    public void testCountWords() throws Exception {

        Pipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

        PCollection<String> input = p.apply(Create.of(line));

        PCollection<String> splitWordsOutput = input.apply(new SplitWords());

        PCollection<KV<String, Long>> countWordsOutput = splitWordsOutput.apply(new CountWords());

        PAssert.that(countWordsOutput).containsInAnyOrder(KV.of("test", 3L),
                KV.of("testing", 3L),
                KV.of("progress", 2L),
                KV.of("done", 1L),
                KV.of("completed", 1L),
                KV.of("in", 2L));

        p.run();
    }
}
