package com.ptransformers;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {

    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {

        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .as(DataflowPipelineOptions.class);

        options.setRunner(DataflowRunner.class);

        Pipeline p = Pipeline.create(options);

        LOG.info("Starting the pipeline...");

        p.apply(Create.of(List.of("Hello", "World")))
                .apply(ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) {
                        LOG.info("element: " + context.element());
                    }
                }));

        p.run();
    }
}
