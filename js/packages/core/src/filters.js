'use strict';

/**
 * httpt template filters (incubation.md §1.3.2).
 *
 * A filter takes a resolved data value and returns the STRING that should be
 * substituted into the template output. Filters never add surrounding quotes
 * unless the JSON representation of the value itself carries them (see
 * `json-value` on a string).
 */

/**
 * Pretty-prints a value the way fixture 002 expects for `json-value`:
 * standard JSON with a single space after `{`, `:`, `,` and before `}` / `]`.
 * Implemented as indented JSON with the indentation newlines collapsed to a
 * single space, which reproduces `{ "theme": "dark", "notifications": false }`.
 * @param {*} value
 * @returns {string}
 */
function spacedJson(value) {
  return JSON.stringify(value, null, 1).replace(/\n\s*/g, ' ');
}

/**
 * Escapes a string for embedding inside a JSON string/key WITHOUT the
 * surrounding double quotes (so it can be concatenated inside template-provided
 * quotes). e.g. `Generic User` -> `Generic User`, `a"b` -> `a\"b`.
 * @param {*} value
 * @returns {string}
 */
function jsonEscapeUnquoted(value) {
  const quoted = JSON.stringify(String(value));
  return quoted.slice(1, -1);
}

/** @type {Record<string, (value: *) => string>} */
const FILTERS = {
  // §1.3.2.1 Core — inject exactly as provided, no escaping.
  raw(value) {
    if (value === null || value === undefined) return '';
    return String(value);
  },

  // §1.3.2.1 Core — percent-encode for URL safety (space -> %20, # -> %23, ...).
  url(value) {
    if (value === null || value === undefined) return '';
    return encodeURIComponent(String(value));
  },

  // §1.3.2.2 JSON — full JSON representation of the value (strings keep quotes).
  'json-value'(value) {
    return spacedJson(value);
  },

  // §1.3.2.2 JSON — escaped for use inside a JSON string, without quotes.
  'json-string'(value) {
    return jsonEscapeUnquoted(value);
  },

  // §1.3.2.2 JSON — escaped for use as a JSON property key, without quotes.
  'json-key'(value) {
    return jsonEscapeUnquoted(value);
  },
};

/**
 * Applies a named filter to a value.
 * @param {string} name - filter name (already trimmed)
 * @param {*} value
 * @returns {string}
 */
function applyFilter(name, value) {
  const fn = FILTERS[name];
  if (!fn) {
    const err = new Error(`Unknown template filter: '${name}'`);
    err.name = 'TemplateSyntaxError';
    throw err;
  }
  return fn(value);
}

module.exports = { FILTERS, applyFilter, spacedJson, jsonEscapeUnquoted };
