package com.ptransformers;

import com.langchainbeam.EmbeddingModelHandler;
import com.langchainbeam.model.BeamEmbedding;
import com.langchainbeam.model.EmbeddingModelBuilder;
import com.langchainbeam.model.EmbeddingOutput;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.lang.reflect.InvocationTargetException;

public class LangchainBeamKeyedEmbedding<KeyT> extends PTransform<PCollection<KV<KeyT, String>>, PCollection<KV<KeyT, EmbeddingOutput>>> {
    private final EmbeddingModelHandler handler;

    public LangchainBeamKeyedEmbedding(EmbeddingModelHandler handler) {
        this.handler = handler;
    }

    @Override
    public PCollection<KV<KeyT, EmbeddingOutput>> expand(PCollection<KV<KeyT, String>> input) {
        return input.apply("Keyed Embedding Transform", ParDo.of(new KeyedEmbeddingDoFn(this.handler)));
    }

    public static <KeyT> LangchainBeamKeyedEmbedding<KeyT> embed(EmbeddingModelHandler modelHandler) {
        if (modelHandler == null) {
            throw new IllegalArgumentException("Handler cannot be null");
        } else {
            return new LangchainBeamKeyedEmbedding<>(modelHandler);
        }
    }

    private class KeyedEmbeddingDoFn extends DoFn<KV<KeyT, String>, KV<KeyT, EmbeddingOutput>> {
        private final EmbeddingModelHandler handler;
        private EmbeddingModelBuilder modelBuilder;
        private EmbeddingModel model;

        public KeyedEmbeddingDoFn(EmbeddingModelHandler handler) {
            this.handler = handler;
        }

        @Setup
        public void setupModel() throws Exception {
            Class<? extends EmbeddingModelBuilder> modelBuilderClass = this.handler.getOptions().getModelBuilderClass();

            try {
                this.modelBuilder = modelBuilderClass.getDeclaredConstructor().newInstance();
            } catch (IllegalArgumentException | InstantiationException | NoSuchMethodException | SecurityException |
                     InvocationTargetException | IllegalAccessException e) {
                throw new Exception("Failed to set up Embedding model due to instantiation error: ", e);
            }

            this.modelBuilder.setOptions(this.handler.getOptions());
            this.model = this.modelBuilder.build();
        }

        @ProcessElement
        public void processElement(
                @Element KV<KeyT, String> element,
                OutputReceiver<KV<KeyT, EmbeddingOutput>> output
        ) {
            final String input = element.getValue();
            final Response<Embedding> response = this.model.embed(input);

            EmbeddingOutput embeddingOutput = EmbeddingOutput.builder()
                    .embedding(new BeamEmbedding(response.content().vector()))
                    .inputElement(input)
                    .build();
            output.output(KV.of(element.getKey(), embeddingOutput));
        }
    }
}
