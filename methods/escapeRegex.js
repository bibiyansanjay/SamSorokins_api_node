/**
 * @module Utils
 * @description All utility methods define here.
 */

/**
 * @method escapeRegex
 * @description Function to escape special characters in a string for use in a regex
 */

export function escapeRegex(string) {
  // Replace special regex characters with their escaped versions
  return string?.trim().replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); // Escapes all special characters
}
