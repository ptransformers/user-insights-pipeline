package com.ptransformers;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
//import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;

import org.apache.beam.sdk.values.KV;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.langchainbeam.LangchainBeam;
import com.langchainbeam.LangchainModelHandler;
import com.langchainbeam.model.LangchainBeamOutput;
import com.langchainbeam.model.openai.OpenAiModelOptions;

public class App {

    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {

        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .as(DataflowPipelineOptions.class);

        // options.setProject("ptransformers");
        // options.setRegion("us-east1");
        options.setStreaming(true);
        options.setRunner(DataflowRunner.class);

        // options.setRunner(DirectRunner.class);

        Pipeline p = Pipeline.create(options);

        LOG.info("Starting the pipeline...");

        OpenAiModelOptions modelOptions = OpenAiModelOptions.builder()
                .modelName("gpt-4o-mini")
                .apiKey("key")
                .build();

        LangchainModelHandler handler = new LangchainModelHandler(modelOptions, Prompts.CONVERSATION_SUMMARY);

        p.apply("ReadFromPubSub", PubsubIO.readStrings()
                .fromSubscription("projects/ptransformers/subscriptions/user-conversation-sub"))
                .apply("ExtractContext", LangchainBeam.run(handler))
                .apply(ParDo.of(new DoFn<LangchainBeamOutput, KV<String, LangchainBeamOutput>>() {

                    @ProcessElement
                    public void process(ProcessContext context) {

                        LOG.info("Model Output: " + context.element().getOutput());

                        context.output(KV.of(java.util.UUID.randomUUID().toString(), context.element()));
                    }
                }));
        p.run();
    }

}
