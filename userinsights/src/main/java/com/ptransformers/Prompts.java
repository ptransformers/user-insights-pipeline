package com.ptransformers;

public class Prompts {

    final static String CONVERSATION_SUMMARY = """
            You should analyse a conversation between a user and an agent.

            and extract personal context and experiences from the user messages only. Focus on details such as:

            - Personal preferences (destinations, activities, foods, accommodations, etc.)
            - Past experiences (places visited, feedback, highlights)
            - Travel goals or intentions
            - Constraints (budget, family, health, timing, etc.)
            - Any other relevant personal insights

            Ignore the agent messages. Do not assume or invent information.

            Provide the extracted user context as a numbered list. Be concise and accurate.
            """;

}
