package com.ptransformers;

import com.langchainbeam.model.LangchainBeamOutput;
import com.langchainbeam.model.LangchainModelBuilder;
import com.langchainbeam.model.LangchainModelOptions;
import com.langchainbeam.model.ModelPrompt;
import dev.langchain4j.model.chat.ChatLanguageModel;
import org.apache.beam.sdk.transforms.DoFn;

import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

/**
 * Abstract base class for Apache Beam DoFn implementations that process input elements
 * using LangChain models.
 *
 * <p>
 * This class provides common functionality for LangChain-based DoFn implementations,
 * including model initialization, prompt formatting, and text generation. It serves
 * as the foundation for specialized DoFn implementations that handle different input
 * processing scenarios.
 * </p>
 *
 * <p>
 * The class is parameterized with a handler type that extends {@link BaseModelHandler},
 * allowing different handler implementations to provide specific configuration and behavior.
 * </p>
 *
 * @param <HandlerT> The type of model handler used by this DoFn
 */
public abstract class BaseLangchainDoFn<HandlerT extends BaseModelHandler, InputT> extends DoFn<InputT, LangchainBeamOutput> {

    protected final HandlerT handler;
    protected ChatLanguageModel model;
    protected LangchainModelBuilder modelBuilder;
    protected String modelOutputFormat;

    /**
     * Constructs a new BaseLangchainDoFn with the specified model handler.
     *
     * @param handler The model handler that provides configuration and behavior for this DoFn
     */
    protected BaseLangchainDoFn(HandlerT handler) {
        this.handler = handler;
    }

    /**
     * Initializes the LangChain model before processing elements.
     * <p>
     * This method retrieves model options from the handler, instantiates a
     * {@link LangchainModelBuilder} based on those options, and configures
     * it to build the model.
     * </p>
     *
     * @throws Exception if an error occurs during model setup, such as
     *                   instantiation or configuration errors
     */
    @Setup
    public void setup() throws Exception {
        LangchainModelOptions options = handler.getOptions();
        Class<? extends LangchainModelBuilder> modelBuilderClass = options.getModelBuilderClass();

        try {
            this.modelBuilder = modelBuilderClass.getDeclaredConstructor().newInstance();
        } catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException
                 | SecurityException | InvocationTargetException e) {
            throw new Exception("Failed to set up Langchain model due to instantiation error: ", e);
        }
        this.modelOutputFormat = Objects.requireNonNullElse(handler.getOutputFormat(), "Plain text");
        this.modelBuilder.setOptions(options);
        this.model = modelBuilder.build();
    }

    /**
     * Formats the final prompt by combining the input element and instruction prompt
     * using the template defined in {@link ModelPrompt#PROMPT}.
     *
     * <p>
     * The final prompt includes the input text, instruction prompt, and the desired output format.
     * </p>
     *
     * @param input  The input text to be processed
     * @param prompt The instruction prompt that guides the model's response
     * @return The formatted prompt string ready for model processing
     */
    protected String getFinalPrompt(String input, String prompt) {
        return String.format(ModelPrompt.PROMPT, input, prompt, this.modelOutputFormat);
    }

    /**
     * Generates text using the configured language model based on the provided prompt.
     *
     * <p>
     * This method handles the actual interaction with the LangChain model, passing the
     * formatted prompt and returning the generated output.
     * </p>
     *
     * @param finalPrompt The formatted prompt to send to the language model
     * @return The text generated by the language model
     * @throws RuntimeException if an error occurs during text generation
     */
    protected String doGenerate(String finalPrompt) {
        try {
            return model.generate(finalPrompt);
        } catch (Exception e) {
            throw e; // Note: Consider wrapping with a more specific exception
        }
    }
}