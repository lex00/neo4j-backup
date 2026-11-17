// Site style rule (`just docs-lint`). Splits prose into sentences with sentence-splitter, the
// package textlint-rule-sentence-length uses, with inline code masked out, and reports:
//   - a sentence, heading, list item or table cell whose first word is "What" or "That";
//   - a negation (not, never, no, n't, without, ...): say what is, in positive terms.
const { split, SentenceSplitterSyntax } = require("sentence-splitter");
const { StringSource } = require("textlint-util-to-string");

const STARTS = ["what", "that"];
const NEGATIONS = ["not", "never", "no", "nothing", "none", "nobody", "nowhere", "neither",
  "nor", "cannot", "without", "unless"];
const NEG = new RegExp(`\\b(${NEGATIONS.join("|")}|[a-z]+n't)\\b`, "gi");

module.exports = function (context) {
  const { Syntax, RuleError, report, locator } = context;
  function check(node) {
    const source = new StringSource(node, {
      replacer({ node: n, maskValue }) {
        if (n.type === Syntax.Code || n.type === Syntax.Html) return maskValue("_");
        return undefined;
      },
    });
    const text = source.toString();
    const at = (i, len) => {
      const a = source.originalIndexFromIndex(i);
      const b = source.originalIndexFromIndex(i + len) ?? a + len;
      return locator.range([a, b]);
    };
    for (const s of split(text)) {
      if (s.type !== SentenceSplitterSyntax.Sentence) continue;
      const str = text.slice(s.range[0], s.range[1]);
      const lead = /^[\s"'(*_]*([A-Za-z]+)/.exec(str);
      if (lead && STARTS.includes(lead[1].toLowerCase())) {
        const i = s.range[0] + lead.index + lead[0].length - lead[1].length;
        report(node, new RuleError(`Starts with "${lead[1]}": lead with the subject or the action.`,
          { padding: at(i, lead[1].length) }));
      }
      for (const m of str.matchAll(NEG)) {
        report(node, new RuleError(`"${m[0]}": state it in positive terms.`,
          { padding: at(s.range[0] + m.index, m[0].length) }));
      }
    }
  }
  return { [Syntax.Paragraph]: check, [Syntax.Header]: check, [Syntax.TableCell]: check };
};
