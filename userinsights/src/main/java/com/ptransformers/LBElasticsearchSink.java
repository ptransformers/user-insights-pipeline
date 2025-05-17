package com.ptransformers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.langchainbeam.EmbeddingModelHandler;
import com.langchainbeam.model.EmbeddingOutput;
import com.langchainbeam.model.LangchainBeamOutput;
import com.langchainbeam.model.openai.OpenAiEmbeddingModelOptions;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Instant;

import java.util.Map;

import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class LBElasticsearchSink extends PTransform<PCollection<KV</*user id*/String, /*lb output*/LangchainBeamOutput>>, PDone> {

    private static final String EMBEDDING_MODEL = "text-embedding-3-small";
    private static final String INDEX_NAME = "user_insights";
    private static final int EMBEDDING_DIMENSION = 1536;

    private final String esEndpoint;
    private final String esApiKey;
    private final EmbeddingModelHandler handler;

    LBElasticsearchSink(String esEndpoint, String esApiKey, String openAiApiKey) {
        this.esEndpoint = esEndpoint;
        this.esApiKey = esApiKey;
        var embeddingModelOptions = OpenAiEmbeddingModelOptions.builder()
                .apikey(openAiApiKey)
                .modelName(EMBEDDING_MODEL)
                .dimensions(EMBEDDING_DIMENSION)
                .build();

        this.handler = new EmbeddingModelHandler(embeddingModelOptions);
    }


    @Override
    public PDone expand(PCollection<KV<String, LangchainBeamOutput>> input) {
        final PCollection<KV<String, EmbeddingOutput>> embeddedPCollection =
                input.apply(MapElements.into(kvs(strings(), strings()))
                                .via((KV<String, LangchainBeamOutput> e) -> {
                                    final String userId = e.getKey();
                                    final LangchainBeamOutput lbOutput = e.getValue();
                                    return KV.of(userId, lbOutput.getOutput());
                                }))
                        .apply(LangchainBeamKeyedEmbedding.embed(this.handler));


        embeddedPCollection.apply("Embedding To Elasticsearch Row", ParDo.of(new EmbeddingToESRowFn()))
                .apply("Write To Elasticsearch", ElasticsearchIO.write()
                        .withConnectionConfiguration(ElasticsearchIO.ConnectionConfiguration.create(
                                new String[]{this.esEndpoint},
                                INDEX_NAME
                        ).withApiKey(this.esApiKey)));

        return PDone.in(input.getPipeline());
    }


    private static class EmbeddingToESRowFn
            extends DoFn<KV<String, EmbeddingOutput>, String> {

        private transient ObjectMapper mapper;

        @Setup
        public void setup() {
            this.mapper = new ObjectMapper();
        }

        @ProcessElement
        public void process(
                @Element KV<String, EmbeddingOutput> element,
                OutputReceiver<String> output
        ) throws JsonProcessingException {
            final Instant conversationAt = Instant.now();
            final String userId = element.getKey();
            final String conversation = element.getValue().getInputElement();
            final float[] conversationVector = element.getValue().getEmbedding().vector();
            var map = Map.of(
                    "user_id", userId,
                    "conversation_at", conversationAt.toString(),
                    "conversation", conversation,
                    "conversation_vector", conversationVector
            );

            final String jsonString = this.mapper.writeValueAsString(map);
            output.output(jsonString);
        }

        @Teardown
        public void teardown() {
            if (this.mapper != null) {
                this.mapper = null;
            }
        }

    }
}
