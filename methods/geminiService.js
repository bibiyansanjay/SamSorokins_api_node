// // geminiService.js
// import { GoogleGenerativeAI } from "@google/generative-ai";

// // Initialize the Gemini client with API key

// export async function generateGeminiContent(prompt) {
//   try {
//     console.log(process.env.GEMINI_API_KEY);
//     const ai = new GoogleGenerativeAI({ apiKey: process.env.GEMINI_API_KEY });
//     const model = ai.getGenerativeModel({ model: "gemini-2.0-flash" });
//     const response = await model.generateContent([
//       {
//         role: "user",
//         parts: [{ text: prompt }],
//       },
//     ]);
//     // const response = await ai.models.generateContent({
//     //   model: "gemini-2.0-flash",
//     //   contents: [
//     //     {
//     //       parts: [{ text: prompt }],
//     //     },
//     //   ],
//     // });

//     // Return the generated text
//     return response.response.text();
//   } catch (error) {
//     console.error("Gemini API Error:", error);
//     throw new Error("Failed to generate content from Gemini API");
//   }
// }

// geminiService.js
import { GoogleGenAI } from "@google/genai";

export async function generateGeminiContent(prompt) {
  try {
    // Initialize the Gemini client with API key
    const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });
    const response = await ai.models.generateContent({
      model: "gemini-2.0-flash",
      contents: [
        {
          parts: [{ text: prompt }],
        },
      ],
    });
    // Return the generated text
    return response?.candidates[0]?.content?.parts[0].text;
  } catch (error) {
    console.error("Gemini API Error:", error);
    throw new Error("Failed to generate content from Gemini API");
  }
}
