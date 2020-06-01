/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pipeline;

import org.apache.beam.runners.spark.SparkContextOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import transforms.PrintFN;

import java.util.Arrays;
import java.util.List;

/** Duplicated from beam-examples-java to avoid dependency. */
public class BeamPipeline {

    /**
    * Concept #2: You can make your pipeline code less verbose by defining your DoFns statically out-
    * of-line. This DoFn tokenizes lines of text into individual words; we pass it to a ParDo in the
    * pipeline.
    */
    @SuppressWarnings("StringSplitter")
    static class ExtractWordsFn extends DoFn<String, String> {
    private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");

        @ProcessElement
        public void processElement(ProcessContext c) {
            if (c.element().trim().isEmpty()) {
                emptyLines.inc();
            }

            // Split the line into words.
            String[] words = c.element().split("[^\\p{L}]+");

            // Output each word encountered into the output PCollection.
            for (String word : words) {
                if (!word.isEmpty()) {
                    c.output(word);
                }
            }
        }
    }

    /** A SimpleFunction that converts a Word and Count into a printable string. */
    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }

    /**
    * A PTransform that converts a PCollection containing lines of text into a PCollection of
    * formatted word counts.
    *
    * <p>Concept #3: This is a custom composite transform that bundles two transforms (ParDo and
    * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
    * modular testing, and an improved monitoring experience.
    */
    public static class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

            // Convert lines of text into individual words.
            PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));

            // Count the number of times each word occurs.
            return words.apply(Count.perElement());
        }
    }

    /**
     *
     * <p>Concept #4: Defining your own configuration options. Here, you can add your own arguments to
     * be processed by the command-line parser, and specify default values for them. You can then
     * access the options values in your pipeline code.
     *
     * <p>Inherits SparkContextOptions configurations, in order to inject Databricks' SparkContext into Beam.
     */
    public interface WordCountOptions extends SparkContextOptions {
        @Description("Mocked Option just to provide an example. Access this option via methods defined below.")
        @Default.String("gs://beam-samples/shakespeare/kinglear.txt")
        String getUnusedOption();
        void setUnusedOption(String value);
    }

    public static void main(String[] args) {
        WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);

        // This line is crucial in order to inject Databricks' SparkContext into Beam.
        // Without setting the SparkContext, Beam is not able to scale on Databricks Clusters.
        // This configuration works if Pipeline is launched with
        // "--runner=SparkRunner" and "--usesProvidedSparkContext" as parameters.
        options.setProvidedSparkContext(JavaSparkContext.fromSparkContext(SparkContext.getOrCreate()));
        // By setting the line above, the pipeline won't work in local mode anymore.
        // Comment it to make this Pipeline work in DirectRunner (local mode) too.

        // Create Pipeline using previous options
        Pipeline p = Pipeline.create(options);

        // This mocked input string may not be sufficiently long in order to trigger cluster scalability
        // Reading a longer input may help scaling over multiple workers.
        // However, the setting provided above guarantees that your Beam Pipeline will scale over multiple workers.
        final List<String> LINES = Arrays.asList(
                "To be, or not to be: that is the question: ",
                "Whether 'tis nobler in the mind to suffer ",
                "The slings and arrows of outrageous fortune, ",
                "Or to take arms against a sea of troubles, ");

        // Concepts #2 and #3: Our pipeline applies the composite CountWords transform, and passes the
        // static FormatAsTextFn() to the ParDo transform.
        p.apply("CreateLines", Create.of(LINES))
                .apply(new CountWords())
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("PrintCounts", ParDo.of(PrintFN.printString));

        p.run().waitUntilFinish();
    }
}
