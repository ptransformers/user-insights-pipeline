package com.ptransformers;

import com.langchainbeam.model.LangchainBeamOutput;
import com.langchainbeam.model.openai.OpenAiModelOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {

    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {

        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .as(DataflowPipelineOptions.class);
        options.setStreaming(true);

        Pipeline p = Pipeline.create(options);

        LOG.info("Starting the pipeline...");

        OpenAiModelOptions modelOptions = OpenAiModelOptions.builder()
                .modelName("gpt-4o-mini")
                .apiKey("<api-key>")
                .build();

        final LangchainDynamicPromptModelHandler handler = new LangchainDynamicPromptModelHandler(modelOptions, "gs://pt-prompt-templates/conversation_summary_prompt")
                .withWatchDurationMinutes(1);


        p.apply("ReadFromPubSub", PubsubIO.readStrings()
                        .fromSubscription("projects/ptransformers/subscriptions/user-conversation-sub"))
                .apply("ExtractContext", LangchainBeam_.runWithDynamicPrompt(handler))
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
