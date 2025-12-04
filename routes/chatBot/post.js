import { generateGeminiContent } from "../../methods/geminiService";
/**
 * @name /chatBot/ POST
 * @memberof module:Routes.chatBot
 * @description Function to handle POST requests of chatBot.
 */

export default async (req, res, next) => {
  try {
    const { prompt } = req.body;

    if (!prompt) {
      return res.status(400).json({ error: "Prompt is required" });
    }

    const result = await generateGeminiContent(prompt);
    res.json({ message: "Response fetched successfully", response: result });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};
