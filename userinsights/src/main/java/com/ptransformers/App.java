package com.ptransformers;

import com.langchainbeam.model.LangchainBeamOutput;
import com.langchainbeam.model.openai.OpenAiModelOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class App {

    private static final String PROMPT_PATH = "gs://pt-prompt-templates/conversation_summary_prompt";
    private static final String PUBSUB_SUBSCRIPTION = "projects/ptransformers/subscriptions/user-conversation-sub";

    public interface LangchainBeamPipelineOptions
            extends PipelineOptions {

        @Validation.Required
        String getOpenAiApiKey();

        void setOpenAiApiKey(String openAiApiKey);

        @Validation.Required
        String getEsEndpoint();

        void setEsEndpoint(String esEndpoint);

        @Validation.Required
        String getEsApiKey();

        void setEsApiKey(String esApiKey);
    }


    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {

        LangchainBeamPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .as(LangchainBeamPipelineOptions.class);

        options.as(StreamingOptions.class).setStreaming(true);

        Pipeline p = Pipeline.create(options);

        LOG.info("Starting the pipeline...");

        OpenAiModelOptions modelOptions = OpenAiModelOptions.builder()
                .modelName("gpt-4o-mini")
                .apiKey(options.getOpenAiApiKey())
                .build();

        final LangchainDynamicPromptModelHandler handler =
                new LangchainDynamicPromptModelHandler(modelOptions, PROMPT_PATH)
                        .withWatchDurationMinutes(1);


        final PCollection<KV<String, LangchainBeamOutput>> keyed =
                p.apply("Read User Conversation", PubsubIO.readStrings()
                                .fromSubscription(PUBSUB_SUBSCRIPTION))
                        .apply("Extract Content Summary With Dynamic Prompt", LangchainBeam_.runWithDynamicPrompt(handler))
                        .apply("To Keyed Format", ParDo.of(new DoFn<LangchainBeamOutput, KV<String, LangchainBeamOutput>>() {
                            private static final String MOCK_USER_ID = UUID.randomUUID().toString();

                            @ProcessElement
                            public void process(ProcessContext context) {
                                LOG.info("Model Output: {}", context.element().getOutput());
                                context.output(KV.of(MOCK_USER_ID, context.element()));
                            }
                        }));

        keyed.apply("Embedding And Sink To ES", new LBElasticsearchSink(
                options.getEsEndpoint(),
                options.getEsApiKey(),
                options.getOpenAiApiKey()
        ));
        p.run();
    }

}
