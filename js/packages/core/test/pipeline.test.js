const fs = require('node:fs');
const path = require('node:path');
const { describe, it } = require('node:test');
const assert = require('node:assert');
const { hydrate, parse } = require('../src/pipeline.js');
const { loadE2eFixtures, E2E_DIR } = require('@httpt/test-utils');

/**
 * Collects an async iterable into an array.
 * @param {AsyncIterable<*>} iterable
 */
async function collect(iterable) {
  const out = [];
  for await (const item of iterable) out.push(item);
  return out;
}

describe('Pipeline: Hydrate & Parse', () => {
  const fixtures = loadE2eFixtures();

  for (const fixture of fixtures) {
    const nameToLog = fixture.irFile || fixture.baseName;

    if (fixture.error) {
      it(`should throw ${fixture.error.name} for ${nameToLog}`, async () => {
        await assert.rejects(
          async () => {
            const { resolvedStream, bodyStream } = hydrate(fixture.template, fixture.data);
            await parse(resolvedStream, bodyStream);
          },
          (err) => err.name === fixture.error.name
        );
      });
      continue;
    }

    it(`should process ${nameToLog} correctly`, async () => {
      const { resolvedStream, bodyStream } = hydrate(fixture.template, fixture.data);
      const { ir } = await parse(resolvedStream, bodyStream);
      assert.deepEqual(ir, fixture.ir);
    });
  }

  // Index Shift Map — only 003/004 are byte-exact oracles (001/002 are
  // hand-authored approximations per the spec), so verify the map there.
  for (const base of ['003-post-text-explicit', '004-post-json-native']) {
    it(`should produce the exact index-shift map for ${base}`, async () => {
      const template = fs.readFileSync(path.join(E2E_DIR, `${base}.httpt`), 'utf8');
      const data = JSON.parse(fs.readFileSync(path.join(E2E_DIR, `${base}.data.json`), 'utf8'));
      const expected = JSON.parse(fs.readFileSync(path.join(E2E_DIR, `${base}.httpt-map`), 'utf8'));

      const { mapStream, bodyStream } = hydrate(template, data);
      const map = await collect(mapStream);
      // Drain the body so the worker completes cleanly.
      await bodyStream.getReader().read().catch(() => {});
      assert.deepEqual(map, expected);
    });
  }
});
